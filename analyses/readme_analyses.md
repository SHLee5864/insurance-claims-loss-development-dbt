# 📘 Mack’s Method — Final Implementation Design Specification  
### (v3.0, English Edition)

---

## 1. Purpose

This document defines the full implementation plan for applying **Mack’s Method (1993, 1994)** on top of the `mart_incurred_triangle` table produced by the dbt pipeline.

### Goals

- Compute Chain-Ladder LDFs  
- Estimate Mack variances (**σ², MSEP, process vs parameter variance**)  
- Produce **Ultimate Loss ± Confidence Interval**  
- Maintain full consistency with **Medium-1 (tail-free Chain-Ladder)**  
- Provide **two core visualizations** for Medium Article #8  
- Deliver reproducible analysis in **Databricks (Python + Notebook)**  

---

## 2. Input Data

### 2.1 Required Table

**`mart_incurred_triangle`**  
Cumulative incurred loss triangle.  
This is the **only required input** for Mack’s Method.

---

### 2.2 Loading Method

**Recommended (for reproducibility):**

1. Export `mart_incurred_triangle` to CSV  

2. Store under:

```bash
analysis/data/incurred_triangle.csv
```

3. Load in notebook:

```python
triangle = pd.read_csv("analysis/data/incurred_triangle.csv")
```

---

## 3. Mathematical Scope

The implementation follows **Mack (1993, 1994)** exactly.

---

### 3.1 LDF Estimation (Volume-Weighted)

\[
\hat{f}_j =
\frac{\sum_i C_{i, j+1}}{\sum_i C_{i, j}}
\]

**Note:**
- Medium-1 used **simple average LDFs**
- Mack requires **volume-weighted LDFs** for:
  - unbiasedness  
  - variance consistency  

**Mandatory sentence for Medium #8:**

> “Medium-1 uses simple average LDFs, while Mack’s Method uses volume-weighted LDFs to ensure consistency with its variance structure.”

---

### 3.2 Variance Estimation (σ²)

\[
\hat{\sigma}_j^2 =
\frac{1}{n_j - 1}
\sum_i
C_{i,j}
\left(
\frac{C_{i,j+1}}{C_{i,j}} - \hat{f}_j
\right)^2
\]

#### Last Development Year Extrapolation (Required)

\[
\sigma_{J-1}^2 =
\min\left(
\frac{\sigma_{J-2}^2}{\sigma_{J-3}},
\;\sigma_{J-2}^2
\right)
\]

---

### 3.3 MSEP (Mean Squared Error of Prediction)

#### Process Variance

\[
PV_i =
\sum_{j = k_i}^{J-1}
\hat{C}_{i,j}^2
\hat{\sigma}_j^2
\prod_{m=j+1}^{J-1}
\hat{f}_m^2
\]

#### Parameter Variance

\[
ParV_i =
\hat{C}_{i,k_i}^2
\sum_{j = k_i}^{J-1}
\frac{\hat{\sigma}_j^2}{\left(\sum C_{i,j}\right)^2}
\prod_{m=j+1}^{J-1}
\hat{f}_m^2
\]

#### Total MSEP

\[
MSEP_i = PV_i + ParV_i
\]

---

### 3.4 Confidence Interval

\[
\hat{U}_i \pm z_{\alpha/2} \sqrt{MSEP_i}
\]

| Confidence Level | z-value |
|------------------|--------|
| 75%              | 1.15   |
| 90%              | 1.645  |
| 95%              | 1.96   |

---

## 4. Tail Factor (Scope Decision)

### 4.1 Medium-1 Consistency

- Medium-1 explicitly uses:  
  **tail = 1.0**

→ Mack implementation must also default to:

```python
tail_factor = 1.0
```

---

### 4.2 Tail Factor Policy

- Provide `compute_tail_factor()` function  
- Default: **1.0**  
- Notebook: include **optional tail sensitivity cell**  
- Medium Article #8: **exclude tail discussion**

---

## 5. Python Structure (Databricks Repo)

```bash
analysis/
  ├─ data/
  │    ├─ incurred_triangle.csv
  │    ├─ expected_ldf.csv
  │    ├─ expected_msep.csv
  │    ├─ expected_sigma2.csv
  │    └─ test_triangle.csv
  ├─ mack_utils.py
  ├─ mack_tests.py
  └─ mack_chain_ladder.ipynb
```

---

## 6. Function Signatures (`mack_utils.py`)

```python
def load_triangle(path): ...

def compute_ldf(triangle): ...

def compute_sigma2(triangle, ldf): ...

def extrapolate_sigma2(sigma2): ...

def compute_msep(triangle, ldf, sigma2): ...

def compute_ci(ultimate, msep, level=0.95): ...

def compute_tail_factor(ldf, method="none"): ...
```

### Default Tail Factor

```python
return 1.0
```

---

## 7. Test Cases (Required)

### Why?

- Mack formulas are **long and error-prone**  
- σ² extrapolation is **tricky**  
- LDF differs from Medium-1  
- Ensures correctness before publishing  

### Required Files

```bash
analysis/data/test_triangle.csv
analysis/data/test_expected_ldf.csv
analysis/data/test_expected_sigma2.csv
analysis/data/test_expected_msep.csv
```

### Test Code (`mack_tests.py`)

```python
def test_ldf(): ...
def test_sigma2(): ...
def test_msep(): ...
```

---

## 8. Notebook Structure (`mack_chain_ladder.ipynb`)

1. Environment setup  
2. Load triangle  
3. Compute LDF  
4. Compute σ² + extrapolation  
5. Compute MSEP  
6. Compute ultimate (tail = 1.0)  
7. Compute CI  
8. Two key visualizations  
9. Tail sensitivity (optional)  
10. Summary + Medium linkage  

---

## 9. Visualizations for Medium Article #8 (Only 2)

- MSEP decomposition (process vs parameter)  
- Ultimate ± CI band  

---

## 10. Final Scope Summary

| Topic            | Decision |
|------------------|----------|
| Tail factor      | Default 1.0 (Medium-1 consistency), optional only |
| LDF method       | Volume-weighted (Mack) |
| σ² extrapolation | Mack convention |
| Tests            | Required |
| Notebook         | Full analysis + optional tail |
| Medium article   | Variance + CI only |

---