<div align="center">

# 🔍 LinkedIn Job Market Analyzer

### Snowflake · Streamlit · Architecture Medallion

*Projet réalisé dans le cadre du cours d'Architecture Big Data*

---

**ACHAB Malik** &nbsp;·&nbsp; **NZONDA NDE Bosco Junior**

</div>

---

## 💡 C'est quoi ce projet ?

On a pris un dataset massif d'offres d'emploi LinkedIn et on l'a transformé en pipeline de données complet sur Snowflake — du fichier brut S3 jusqu'aux visualisations interactives — en suivant l'architecture **Medallion** (Bronze → Silver → Gold).

> *En clair : des données sales, des erreurs de types, des JSON mal structurés, des jointures qui ne matchent pas... et on a tout résolu.*

---

## 👥 Équipe

| | Nom | Prénom | Rôle principal |
|--|-----|--------|----------------|
| 🧑‍💻 | ACHAB | Malik | Pipeline SQL, Analyses 2 & 3, Debug jointures, Documentation |
| 🧑‍💻 | NZONDA NDE | Bosco Junior | Analyses 1 & 4, Reconstruction Silver |

---

## 🗂️ Structure du dépôt

```
📁 projet-linkedin-snowflake/
│
├── 📄 README.md
│
├── 📁 sql/
│   └── linkedin_sql_final_v2.sql       ← Pipeline complet Bronze → Silver → Gold
│
└── 📁 streamlit/
    └── streamlit_linkedin_final_v3.py  ← Les 5 analyses interactives
```

---

## 🏗️ Architecture Medallion

```
         ☁️  AWS S3  (snowflake-lab-bucket)
                │
        ┌───────┴────────┐
        │   INGESTION     │  COPY INTO + FILE FORMAT
        └───────┬────────┘
                │
        ┌───────▼────────────────────────────────────────┐
        │  🥉  BRONZE  —  Données brutes                  │
        │  ├── JOB_POSTINGS.csv                           │
        │  ├── COMPANIES.json          ← tableau JSON     │
        │  ├── JOB_SKILLS.csv                             │
        │  ├── BENEFITS.csv                               │
        │  ├── EMPLOYEE_COUNTS.csv                        │
        │  ├── JOB_INDUSTRIES.json     ← tableau JSON     │
        │  ├── COMPANY_SPECIALITIES.json                  │
        │  └── COMPANY_INDUSTRIES.json ← tableau JSON     │
        └───────┬────────────────────────────────────────┘
                │  TRY_CAST · TRY_TO_NUMBER · TO_TIMESTAMP
                │  STRIP_OUTER_ARRAY · LATERAL FLATTEN
        ┌───────▼────────────────────────────────────────┐
        │  🥈  SILVER  —  Données nettoyées et typées     │
        │  ├── JOB_POSTINGS  (company_id extrait, dates)  │
        │  ├── COMPANIES     (NUMBER::STRING pour l'ID)   │
        │  ├── JOB_INDUSTRIES                             │
        │  ├── COMPANY_INDUSTRIES  (FLATTEN + cast)       │
        │  └── ...                                        │
        └───────┬────────────────────────────────────────┘
                │  JOIN · GROUP BY · ROW_NUMBER · COALESCE
        ┌───────▼────────────────────────────────────────┐
        │  🥇  GOLD  —  Prêt pour l'analyse              │
        │  ├── DIM_JOBS          (vue)                    │
        │  ├── DIM_COMPANIES     (vue)                    │
        │  ├── FACT_JOBS         (avec job_score)         │
        │  ├── TOP_SKILLS_BY_INDUSTRY                     │
        │  └── FACT_BENEFITS                              │
        └───────┬────────────────────────────────────────┘
                │
        ┌───────▼────────┐
        │   STREAMLIT     │  5 analyses interactives
        └────────────────┘
```

---

## 🧱 Commandes SQL — avec explications

### Étape 0 · Initialisation

```sql
CREATE DATABASE IF NOT EXISTS LINKEDIN;
CREATE OR REPLACE SCHEMA LINKEDIN.BRONZE;
CREATE OR REPLACE SCHEMA LINKEDIN.SILVER;
CREATE OR REPLACE SCHEMA LINKEDIN.GOLD;

CREATE OR REPLACE STAGE LINKEDIN_STAGE
    URL = 's3://snowflake-lab-bucket/';

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE                         = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER                  = 1
    NULL_IF                      = ('NULL', 'null', '', 'N/A')
    EMPTY_FIELD_AS_NULL          = TRUE;
```

> `NULL_IF` transforme les chaînes vides et `'NULL'` en vraies valeurs `NULL` dès l'ingestion — ça évite de les gérer à chaque requête ensuite.

---

### Étape 1 · Bronze — Ingestion brute

#### Fichiers CSV — tout en STRING

```sql
CREATE OR REPLACE TABLE LINKEDIN.BRONZE.JOB_POSTINGS (
    job_id       STRING,
    company_name STRING,   -- contient en réalité un company_id numérique !
    title        STRING,
    max_salary   STRING,   -- sera casté en FLOAT en Silver
    ...
);

COPY INTO LINKEDIN.BRONZE.JOB_POSTINGS
FROM @LINKEDIN_STAGE/job_postings.csv
FILE_FORMAT = CSV_FORMAT;
```

> Toutes les colonnes sont en `STRING` au Bronze. Si on caste directement et qu'une valeur est invalide, le `COPY INTO` échoue sur toute la ligne. Le typage se fait proprement en Silver avec `TRY_CAST`.

#### Fichiers JSON — STRIP_OUTER_ARRAY = TRUE

```sql
CREATE OR REPLACE TABLE LINKEDIN.BRONZE.COMPANIES (DATA VARIANT);

COPY INTO LINKEDIN.BRONZE.COMPANIES
FROM @LINKEDIN_STAGE/companies.json
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
FORCE = TRUE;
```

> **Point critique.** Le fichier JSON est un tableau `[{...}, {...}]`. Sans `STRIP_OUTER_ARRAY = TRUE`, Snowflake charge tout en **une seule ligne** (`IS_ARRAY = TRUE`) et `DATA:company_id` retourne NULL partout. Avec ce paramètre, chaque objet devient une ligne (`IS_OBJECT = TRUE`). `FORCE = TRUE` force le rechargement même si le fichier avait déjà été chargé.

---

### Étape 2 · Silver — Nettoyage et typage

#### JOB_POSTINGS — les transformations clés

```sql
CREATE OR REPLACE TABLE LINKEDIN.SILVER.JOB_POSTINGS AS
SELECT
    TRIM(job_id)                                                AS job_id,

    -- company_name contient un ID numérique (ex: 54844.0), pas un nom
    TRY_TO_NUMBER(TRIM(company_name))                           AS company_id,

    -- timestamps en millisecondes → diviser par 1000
    TO_TIMESTAMP(TRY_CAST(listed_time AS NUMBER) / 1000)        AS listed_time,

    -- booléens stockés comme chaînes ('TRUE', 'true', 'True'...)
    IFF(UPPER(TRIM(remote_allowed)) = 'TRUE', TRUE, FALSE)      AS remote_allowed,

    -- salaires STRING → FLOAT (TRY_CAST retourne NULL si invalide)
    TRY_CAST(max_salary AS FLOAT)                               AS max_salary,

    -- tranche salariale calculée
    CASE
        WHEN TRY_CAST(max_salary AS FLOAT) IS NULL  THEN 'unknown'
        WHEN TRY_CAST(max_salary AS FLOAT) < 40000  THEN 'low'
        WHEN TRY_CAST(max_salary AS FLOAT) <= 90000 THEN 'medium'
        ELSE                                              'high'
    END                                                         AS salary_range,

    -- nombre de competences listees
    ARRAY_SIZE(SPLIT(skills_desc, ','))                         AS num_skills

FROM LINKEDIN.BRONZE.JOB_POSTINGS
WHERE TRIM(job_id) IS NOT NULL AND TRIM(job_id) != '';
```

#### COMPANIES — le double cast NUMBER::STRING

```sql
CREATE OR REPLACE TABLE LINKEDIN.SILVER.COMPANIES AS
SELECT
    DATA:company_id::NUMBER::STRING     AS company_id,  -- NUMBER d'abord, STRING ensuite
    DATA:name::STRING                   AS company_name,
    DATA:company_size::NUMBER::STRING   AS company_size,
    DATA:country::STRING                AS country,
    DATA:city::STRING                   AS city,
    DATA:url::STRING                    AS url
FROM LINKEDIN.BRONZE.COMPANIES
WHERE IS_OBJECT(DATA)
  AND DATA:company_id IS NOT NULL;
```

> Dans le JSON, `company_id` est un entier brut (`1009` sans guillemets). Le cast `::NUMBER::STRING` force la lecture comme nombre puis la conversion en chaîne propre — ce qui garantit que `'1009'` dans COMPANIES matche `'1009'` dans JOB_POSTINGS.

#### COMPANY_INDUSTRIES — LATERAL FLATTEN

```sql
CREATE OR REPLACE TABLE LINKEDIN.SILVER.COMPANY_INDUSTRIES AS
SELECT
    f.value:company_id::NUMBER::STRING  AS company_id,
    f.value:industry::STRING            AS industry_id
FROM LINKEDIN.BRONZE.COMPANY_INDUSTRIES,
LATERAL FLATTEN(input => DATA) f        -- dérouler le tableau JSON en lignes
WHERE f.value:company_id IS NOT NULL;
```

> `LATERAL FLATTEN` transforme chaque élément d'un tableau JSON en une ligne distincte. Indispensable quand le fichier entier est stocké en une seule ligne VARIANT.

---

### Étape 3 · Gold — Analyses

#### FACT_JOBS avec job_score composite

```sql
CREATE OR REPLACE TABLE LINKEDIN.GOLD.FACT_JOBS AS
SELECT
    jp.*,
    c.company_name,
    c.company_size,
    (
        COALESCE(jp.max_salary, 0) * 0.4   -- salaire      : 40%
        + COALESCE(jp.views, 0)    * 0.3   -- visibilite   : 30%
        + COALESCE(jp.applies, 0)  * 0.2   -- attractivite : 20%
        + jp.num_skills * 50               -- richesse en competences
        + IFF(jp.remote_allowed, 1000, 0)  -- bonus remote
    ) AS job_score
FROM LINKEDIN.SILVER.JOB_POSTINGS jp
LEFT JOIN LINKEDIN.SILVER.COMPANIES c
    ON jp.company_id::STRING = c.company_id;
```

---

### Étape 4 · Requetes analytiques

#### Analyse 1 — Top 10 postes par industrie

```sql
WITH ranked AS (
    SELECT
        ji.industry_id,
        jp.title,
        COUNT(*) AS nb_offres,
        ROW_NUMBER() OVER (
            PARTITION BY ji.industry_id  -- classement independant par industrie
            ORDER BY COUNT(*) DESC
        ) AS rn
    FROM LINKEDIN.SILVER.JOB_POSTINGS jp
    JOIN LINKEDIN.SILVER.JOB_INDUSTRIES ji ON jp.job_id = ji.job_id
    WHERE jp.title IS NOT NULL AND jp.title != ''
    GROUP BY ji.industry_id, jp.title
)
SELECT industry_id, title, nb_offres
FROM ranked
WHERE rn <= 10  -- top 10 de chaque industrie
ORDER BY industry_id, nb_offres DESC;
```

#### Analyse 2 — Salaires par industrie

```sql
WITH salaires AS (
    SELECT
        ji.industry_id,
        jp.title,
        ROUND(AVG(NULLIF(jp.med_salary, 0)), 0) AS avg_med_salary,
        -- NULLIF exclut les 0 de la moyenne
        ROW_NUMBER() OVER (
            PARTITION BY ji.industry_id
            ORDER BY AVG(NULLIF(jp.med_salary, 0)) DESC NULLS LAST
        ) AS rn
    FROM LINKEDIN.SILVER.JOB_POSTINGS jp
    JOIN LINKEDIN.SILVER.JOB_INDUSTRIES ji ON jp.job_id = ji.job_id
    WHERE jp.med_salary IS NOT NULL AND jp.med_salary > 0
    GROUP BY ji.industry_id, jp.title
)
SELECT industry_id, title, avg_med_salary
FROM salaires WHERE rn <= 10;
```

#### Analyse 3 — Taille d'entreprise

```sql
SELECT
    CASE c.company_size
        WHEN '1' THEN '1 - Individuel'
        WHEN '2' THEN '2 - 2 a 10 emp.'
        WHEN '3' THEN '3 - 11 a 50 emp.'
        WHEN '4' THEN '4 - 51 a 200 emp.'
        WHEN '5' THEN '5 - 201 a 500 emp.'
        WHEN '6' THEN '6 - 501 a 1000 emp.'
        WHEN '7' THEN '7 - 1K a 5K emp.'
        WHEN '8' THEN '8 - 5K a 10K emp.'
        WHEN '9' THEN '9 - Plus de 10K emp.'
        ELSE          'Non renseigne'
    END AS taille_entreprise,
    COUNT(jp.job_id) AS nb_offres,
    -- pourcentage calcule en une seule requete grace a la fenetre globale
    ROUND(COUNT(jp.job_id) * 100.0 / SUM(COUNT(jp.job_id)) OVER (), 2) AS pourcentage
FROM LINKEDIN.SILVER.JOB_POSTINGS jp
LEFT JOIN LINKEDIN.SILVER.COMPANIES c
    ON jp.company_id::STRING = c.company_id
GROUP BY taille_entreprise
ORDER BY nb_offres DESC;
```

#### Analyse 4 — Secteur d'activite

```sql
SELECT
    COALESCE(NULLIF(TRIM(ci.industry_id), ''), 'Secteur inconnu') AS secteur,
    COUNT(DISTINCT jp.job_id)               AS nb_offres,
    COUNT(DISTINCT c.company_name)          AS nb_entreprises,
    ROUND(AVG(NULLIF(jp.med_salary, 0)), 0) AS salaire_median_moyen
FROM LINKEDIN.SILVER.JOB_POSTINGS jp
LEFT JOIN LINKEDIN.SILVER.COMPANIES c
    ON jp.company_id::STRING = c.company_id
LEFT JOIN LINKEDIN.SILVER.COMPANY_INDUSTRIES ci
    ON c.company_id = ci.company_id
GROUP BY secteur
ORDER BY nb_offres DESC
LIMIT 25;
```

#### Analyse 5 — Type d'emploi

```sql
SELECT
    COALESCE(NULLIF(TRIM(formatted_work_type), ''), 'Non specifie') AS type_emploi,
    COUNT(*) AS nb_offres,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pourcentage,
    ROUND(AVG(NULLIF(med_salary, 0)), 0)   AS salaire_median,
    SUM(IFF(remote_allowed = TRUE, 1, 0))  AS nb_remote
FROM LINKEDIN.SILVER.JOB_POSTINGS
GROUP BY type_emploi
ORDER BY nb_offres DESC;
```

---

## 🐛 Problèmes rencontrés & solutions

### 🔴 Problème 1 — SILVER.COMPANIES vide (0 lignes)

| | |
|--|--|
| **Symptôme** | `SELECT COUNT(*) FROM SILVER.COMPANIES` retourne `0` |
| **Cause** | `companies.json` est un tableau JSON `[{}, {}]`. Sans `STRIP_OUTER_ARRAY = TRUE`, Snowflake charge tout en 1 ligne avec `IS_ARRAY = TRUE`. Le filtre `WHERE IS_OBJECT(DATA)` retourne alors 0 résultats. |
| **Solution** | `FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE) FORCE = TRUE` |

---

### 🔴 Problème 2 — Jointure company_id = 0 match

| | |
|--|--|
| **Symptôme** | `JOIN ON company_name = company_name` retourne `0` lignes |
| **Cause** | `company_name` dans JOB_POSTINGS contient un ID numerique (`54844.0`), pas un nom. Dans COMPANIES, `company_id` est un entier JSON sans guillemets. Les types ne matchaient pas. |
| **Solution** | `DATA:company_id::NUMBER::STRING` dans COMPANIES + `jp.company_id::STRING = c.company_id` en jointure |

---

### 🔴 Problème 3 — COMPANY_INDUSTRIES vide

| | |
|--|--|
| **Symptôme** | `SELECT COUNT(*) FROM SILVER.COMPANY_INDUSTRIES` retourne `0` |
| **Cause** | Le fichier JSON etait charge en 1 seule ligne VARIANT contenant le tableau entier |
| **Solution** | `LATERAL FLATTEN(input => DATA) f` + `f.value:company_id::NUMBER::STRING` |

---

### 🟡 Problème 4 — SyntaxError Streamlit

| | |
|--|--|
| **Symptôme** | `SyntaxError: unterminated string literal (detected at line 69)` |
| **Cause** | Apostrophes dans les labels Python a l'interieur de strings delimitees par des apostrophes |
| **Solution** | Supprimer tous les apostrophes : `d'entreprise` → `d entreprise` |

---

### 🟡 Problème 5 — invalid identifier 'JP.COMPANY_ID'

| | |
|--|--|
| **Symptôme** | `SQL compilation error: invalid identifier 'JP.COMPANY_ID'` |
| **Cause** | `SILVER.JOB_POSTINGS` avait ete creee sans la colonne `company_id` |
| **Solution** | Recreer la table avec `TRY_TO_NUMBER(TRIM(company_name)) AS company_id` |

---

### 🟡 Problème 6 — Timestamps illisibles

| | |
|--|--|
| **Symptôme** | `listed_time = 1701234567000` au lieu d'une vraie date |
| **Cause** | Les timestamps sont en millisecondes (epoch ms) |
| **Solution** | `TO_TIMESTAMP(TRY_CAST(listed_time AS NUMBER) / 1000)` |

---

## 📊 Visualisations Streamlit

| # | Analyse | Graphique | Interactivite |
|---|---------|-----------|---------------|
| 1 | Top postes par industrie | Bar chart horizontal | Selectbox industrie |
| 2 | Meilleurs salaires | Bar chart + fourchette min-max | Selectbox industrie |
| 3 | Taille d'entreprise | Donut + Bar chart | — |
| 4 | Secteur d'activite | Bar chart (couleur = salaire) | Slider top N |
| 5 | Type d'emploi | Donut + Bar chart dynamique | Selectbox metrique |

### Pattern commun a toutes les analyses

```python
from snowflake.snowpark.context import get_active_session
session = get_active_session()  # connexion automatique dans Snowflake SiS

@st.cache_data          # mise en cache pour eviter les rechargements Snowflake
def load_data():
    return session.sql("SELECT ...").to_pandas()

df = load_data()
chart = alt.Chart(df).mark_bar().encode(...)
st.altair_chart(chart, use_container_width=True)
```

---

## 🛠️ Stack technique

| Outil | Usage |
|-------|-------|
| **Snowflake** | Data warehouse, SQL, Streamlit in Snowflake |
| **AWS S3** | Stockage des fichiers sources |
| **Python / Streamlit** | Visualisations interactives |
| **Altair** | Graphiques declaratifs |
| **Architecture Medallion** | Bronze / Silver / Gold |

---

<div align="center">

*Projet Architecture Big Data — ACHAB Malik & NZONDA NDE Bosco Junior*

</div>
