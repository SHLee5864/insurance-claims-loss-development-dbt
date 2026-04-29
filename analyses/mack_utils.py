"""
mack_utils.py — Mack's Chain-Ladder Method (1993, 1994)
 
Implements the distribution-free variance estimation framework
for loss reserving, as described in:
 
    Mack, T. (1993). "Distribution-free calculation of the standard error
    of chain ladder reserve estimates." ASTIN Bulletin, 23(2), 213–225.
 
    Mack, T. (1994). "Which stochastic model is underlying the chain ladder
    method?" Insurance: Mathematics and Economics, 15(2–3), 133–138.
 
This module provides:
    - Volume-weighted LDF estimation
    - Mack variance (σ²) estimation with last-year extrapolation
    - MSEP decomposition (process variance + parameter variance)
    - Normal-approximation confidence intervals
    - Optional tail factor (default = 1.0 for Medium-1 consistency)
 
Input:
    Cumulative incurred triangle as pandas DataFrame.
    Columns: AY, DY1, DY2, ..., DYJ
    Lower-right cells are NaN (unobserved).
 
Usage:
    from mack_utils import (
        load_triangle, compute_ldf, compute_sigma2,
        extrapolate_sigma2, compute_msep, compute_ci
    )
 
    triangle = load_triangle("incurred_triangle.csv")
    ldf      = compute_ldf(triangle)
    sigma2   = extrapolate_sigma2(compute_sigma2(triangle, ldf))
    msep_df  = compute_msep(triangle, ldf, sigma2)
"""

import pandas as pd
import numpy as np
import os


# ------------------------------------------------------------
# 1. Load Triangle (Databricks Repo path aware)
# ------------------------------------------------------------
def load_triangle(filename: str) -> pd.DataFrame:
    """
    Load a cumulative incurred triangle from CSV.
 
    Expected CSV format:
        AY,DY1,DY2,DY3,...,DYJ
        2015,1000,1500,1700,...,1800
        2016,1100,1600,...,NaN
        ...
 
    Parameters
    ----------
    filename : str
        CSV filename located in analyses/data/
 
    Returns
    -------
    pd.DataFrame
        Triangle with AY column and DY1..DYJ columns.
    """
    full_path = os.path.join(os.getcwd(), "data", filename)
    df = pd.read_csv(full_path)
    return df


# ------------------------------------------------------------
# 2. Compute LDF (volume-weighted)
# ------------------------------------------------------------
def compute_ldf(triangle: pd.DataFrame) -> pd.Series:
    """
    Compute volume-weighted Loss Development Factors.
 
    Formula (Mack 1993):
        f_j = Σ C_{i,j+1} / Σ C_{i,j}
 
    where the sum runs over accident years i that have
    observations at both dev_year j and j+1.
 
    Note: Medium-1 used simple average LDF. Mack's Method
    requires volume-weighted LDF for variance consistency.
 
    Parameters
    ----------
    triangle : pd.DataFrame
        Cumulative incurred triangle with AY index.
 
    Returns
    -------
    pd.Series
        LDF indexed by dev_year (1-based).
        ldf[j] = f_j (transition from DY_j to DY_{j+1}).
    """
    df = triangle.copy().set_index("AY")
    dev_cols = [c for c in df.columns if c.startswith("DY")]
    J = len(dev_cols)

    ldfs = {}
    for j in range(J - 1):
        col_j = dev_cols[j]
        col_j1 = dev_cols[j + 1]
        valid = df[[col_j, col_j1]].dropna()
        num = valid[col_j1].sum()
        den = valid[col_j].sum()
        ldfs[j + 1] = num / den

    return pd.Series(ldfs)

# ------------------------------------------------------------
# 3. Compute sigma^2 (Mack variance)
# ------------------------------------------------------------
def compute_sigma2(triangle: pd.DataFrame, ldf: pd.Series) -> pd.Series:
    """
    Estimate the variance parameters σ²_j (Mack 1993).
 
    Formula:
        σ²_j = (1 / (n_j - 1)) × Σ C_{i,j} × (C_{i,j+1}/C_{i,j} - f_j)²
 
    For the last development year (j = J-1), n_j = 1,
    so σ² cannot be estimated directly → returns NaN.
    Use extrapolate_sigma2() to handle this.
 
    Parameters
    ----------
    triangle : pd.DataFrame
    ldf : pd.Series
        Output of compute_ldf().
 
    Returns
    -------
    pd.Series
        σ² indexed by dev_year (1-based).
        Last entry is NaN if only 1 observation exists.
    """
    df = triangle.copy().set_index("AY")
    dev_cols = [c for c in df.columns if c.startswith("DY")]
    J = len(dev_cols)

    sigma2 = {}
    for j in range(J - 1):
        col_j = dev_cols[j]
        col_j1 = dev_cols[j + 1]
        valid = df[[col_j, col_j1]].dropna()
        n_j = len(valid)

        if n_j <= 1:
            sigma2[j + 1] = np.nan
            continue

        f_j = ldf[j + 1]
        ratios = valid[col_j1] / valid[col_j]
        terms = valid[col_j] * (ratios - f_j) ** 2
        sigma2[j + 1] = terms.sum() / (n_j - 1)

    return pd.Series(sigma2)


# ------------------------------------------------------------
# 4. Extrapolate sigma^2 for last development year
# ------------------------------------------------------------
def extrapolate_sigma2(sigma2: pd.Series) -> pd.Series:
    """
    Extrapolate σ² for the last development year where
    direct estimation is impossible (n_j = 1).
 
    Mack (1993) convention:
        σ²_{J-1} = min( σ²_{J-2}² / σ²_{J-3},  σ²_{J-2} )
 
    This assumes the σ² sequence is decreasing. The min()
    ensures the extrapolated value does not exceed σ²_{J-2}.
 
    Parameters
    ----------
    sigma2 : pd.Series
        Output of compute_sigma2(). Last entry may be NaN.
 
    Returns
    -------
    pd.Series
        σ² with last entry filled via extrapolation.
    """
    s = sigma2.copy()
    J = len(s) + 1

    j_last = J - 1
    j_prev = J - 2
    j_prev2 = J - 3

    if pd.isna(s[j_last]):
        ratio = s[j_prev] ** 2 / s[j_prev2]
        s[j_last] = min(ratio, s[j_prev])

    return s

# ------------------------------------------------------------
# 5. Compute MSEP
# ------------------------------------------------------------
def compute_msep(triangle: pd.DataFrame, ldf: pd.Series, sigma2: pd.Series) -> pd.DataFrame:
    """
    Compute Mean Squared Error of Prediction per accident year.
 
    Mack (1993) MSEP formula:
        MSEP(Û_i) = Û_i² × Σ_{j=k_i}^{J-1} (σ²_j / f_j²) × (1/Ĉ_{i,j} + 1/S_j)
 
    where:
        Û_i    = ultimate loss estimate for AY i
        k_i    = last observed dev_year for AY i
        Ĉ_{i,j} = projected cumulative at dev_year j
        S_j    = Σ C_{l,j} for all AYs observed at dev_year j (and j+1)
        f_j    = volume-weighted LDF at dev_year j
 
    The first term (1/Ĉ_{i,j}) represents process variance —
    inherent randomness in future loss development.
 
    The second term (1/S_j) represents parameter variance —
    uncertainty from estimating LDFs with limited data.
 
    Parameters
    ----------
    triangle : pd.DataFrame
    ldf : pd.Series
    sigma2 : pd.Series
        Output of extrapolate_sigma2().
 
    Returns
    -------
    pd.DataFrame
        Columns: AY, ultimate, process_var, parameter_var, msep
    """
    df = triangle.copy().set_index("AY")
    dev_cols = [c for c in df.columns if c.startswith("DY")]
    J = len(dev_cols)

    results = []

    for ay in df.index:
        row = df.loc[ay]
        observed = row.dropna()
        k_i = len(observed)
        last_col = dev_cols[k_i - 1]
        C_last = row[last_col]

        # projected cumulative values
        C_proj = [C_last]
        for j in range(k_i, J):
            C_proj.append(C_proj[-1] * ldf[j])

        ultimate = C_proj[-1]

        # MSEP using combined formula
        PV = 0.0
        ParV = 0.0

        for idx, j in enumerate(range(k_i, J)):
            C_ij = C_proj[idx]
            sigma_j = sigma2[j]
            f_j = ldf[j]

            # S_j = sum of all observed C_{l,j} at dev_year j
            valid = df[[dev_cols[j - 1], dev_cols[j]]].dropna()
            S_j = valid[dev_cols[j - 1]].sum()

            ratio = sigma_j / (f_j ** 2)

            PV += ratio * (1.0 / C_ij)
            ParV += ratio * (1.0 / S_j)

        PV *= (ultimate ** 2)
        ParV *= (ultimate ** 2)

        results.append({
            "AY": ay,
            "ultimate": ultimate,
            "process_var": PV,
            "parameter_var": ParV,
            "msep": PV + ParV
        })

    return pd.DataFrame(results)

# ------------------------------------------------------------
# 6. Confidence Interval
# ------------------------------------------------------------
def compute_ci(ultimate: float, msep: float, level: float = 0.95) -> tuple:
    """
    Compute confidence interval using normal approximation.
 
    Formula (Mack 1993):
        Û ± z_{α/2} × √MSEP
 
    Parameters
    ----------
    ultimate : float
        Point estimate of ultimate loss.
    msep : float
        Mean Squared Error of Prediction.
    level : float
        Confidence level. Supported: 0.75, 0.90, 0.95.
 
    Returns
    -------
    tuple
        (lower_bound, upper_bound)
    """
    z = {0.75: 1.15, 0.90: 1.645, 0.95: 1.96}[level]
    se = np.sqrt(msep)
    return ultimate - z * se, ultimate + z * se


# ------------------------------------------------------------
# 7. Tail Factor (default = 1.0)
# ------------------------------------------------------------
def compute_tail_factor(ldf: pd.Series, method: str = "none") -> float:
    """
    Compute tail factor for development beyond the last observed year.
 
    Default is 1.0 (no tail), consistent with Medium-1 assumptions.
    This function is provided for extensibility; tail estimation
    methods (log-linear extrapolation, industry benchmarks) can be
    added as needed.
 
    Parameters
    ----------
    ldf : pd.Series
        Output of compute_ldf().
    method : str
        "none" (default) → returns 1.0
 
    Returns
    -------
    float
        Tail factor (multiplicative).
    """
    if method == "none":
        return 1.0
    return 1.0
