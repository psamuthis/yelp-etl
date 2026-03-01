
-- REQUETES OLAP
-- Cas d'usage : Cabinet Private Equity
-- Schema DW :
--   FACT_PERFORMANCE    (business_id, time_id, location_id, review_count, tip_count, checkin_count, score_mean)
--   DIM_LOCATION        (location_id, business_id, address, city, postal_code, state, latitude, longitude)
--   DIM_CALENDAR        (ID_Temps, year, month, quarter)
--   DIM_CATEGORY        (category_id, category_name)
--   BRIDGE_BUSINESS_CATEGORY (business_id, category_id)
--   DIM_BUSINESS        (name, stars, is_open, attributes)  -- pas de business_id : non joinable


-- ============================================================
-- Q1 : ROLLUP - Nombre total d'avis et note moyenne
--       par etat > annee, avec sous-totaux a chaque niveau
-- ============================================================
SELECT
    NVL(l.state,        'TOTAL_GLOBAL') AS etat,
    NVL(TO_CHAR(c.year),'TOTAL_ANNEE')  AS annee,
    COUNT(DISTINCT f.business_id)       AS nb_commerces,
    SUM(f.review_count)                 AS total_avis,
    SUM(f.checkin_count)                AS total_checkins,
    ROUND(AVG(f.score_mean), 2)         AS note_moyenne
FROM FACT_PERFORMANCE f
JOIN DIM_LOCATION l ON f.location_id = l.location_id
JOIN DIM_CALENDAR c ON f.time_id     = c.ID_Temps
WHERE f.score_mean IS NOT NULL
GROUP BY ROLLUP(l.state, c.year)
ORDER BY l.state NULLS LAST, c.year NULLS LAST;


-- ============================================================
-- Q2 : CUBE - Analyse croisee etat x categorie x annee
--      avec tous les sous-totaux possibles
-- ============================================================
SELECT
    NVL(l.state,         'TOUS_ETATS')      AS etat,
    NVL(cat.category_name,'TOUTES_CATEGORIES') AS categorie,
    NVL(TO_CHAR(c.year), 'TOUTES_ANNEES')   AS annee,
    COUNT(DISTINCT f.business_id)            AS nb_commerces,
    SUM(f.review_count)                      AS total_avis,
    ROUND(AVG(f.score_mean), 2)              AS note_moyenne
FROM FACT_PERFORMANCE f
JOIN DIM_LOCATION          l   ON f.location_id  = l.location_id
JOIN DIM_CALENDAR          c   ON f.time_id       = c.ID_Temps
JOIN BRIDGE_BUSINESS_CATEGORY bbc ON f.business_id = bbc.business_id
JOIN DIM_CATEGORY          cat ON bbc.category_id  = cat.category_id
WHERE f.score_mean IS NOT NULL
GROUP BY CUBE(l.state, cat.category_name, c.year)
ORDER BY l.state NULLS LAST, cat.category_name NULLS LAST, c.year NULLS LAST;


-- ============================================================
-- Q3 : RANK - Classement des villes par note moyenne
--      avec rang global et rang par etat
-- ============================================================
SELECT
    l.city,
    l.state,
    COUNT(DISTINCT f.business_id)                                       AS nb_commerces,
    SUM(f.review_count)                                                  AS total_avis,
    ROUND(AVG(f.score_mean), 2)                                          AS note_moyenne,
    RANK()       OVER (PARTITION BY l.state ORDER BY AVG(f.score_mean) DESC) AS rang_dans_etat,
    DENSE_RANK() OVER (ORDER BY AVG(f.score_mean) DESC)                  AS rang_global
FROM FACT_PERFORMANCE f
JOIN DIM_LOCATION l ON f.location_id = l.location_id
WHERE f.score_mean IS NOT NULL
GROUP BY l.city, l.state
ORDER BY rang_global;


-- ============================================================
-- Q4 : TOP-K - Top 20 des "pepites" (note >= 4.2, peu connus)
--      Criteres : note moyenne >= 4.2 ET total avis < 150
-- ============================================================
SELECT *
FROM (
    SELECT
        f.business_id,
        l.city,
        l.state,
        SUM(f.review_count)          AS total_avis,
        SUM(f.checkin_count)         AS total_checkins,
        SUM(f.tip_count)             AS total_tips,
        ROUND(AVG(f.score_mean), 2)  AS note_moyenne,
        RANK() OVER (ORDER BY AVG(f.score_mean) DESC, SUM(f.review_count) ASC) AS rang
    FROM FACT_PERFORMANCE f
    JOIN DIM_LOCATION l ON f.location_id = l.location_id
    WHERE f.score_mean IS NOT NULL
    GROUP BY f.business_id, l.city, l.state
    HAVING AVG(f.score_mean) >= 4.2
       AND SUM(f.review_count) < 150
)
WHERE rang <= 20
ORDER BY rang;


-- ============================================================
-- Q5 : FENETRAGE TEMPOREL - Evolution du nombre d'avis
--      par trimestre et par etat, avec comparaison trimestre precedent
-- ============================================================
SELECT
    l.state,
    c.year,
    c.quarter,
    SUM(f.review_count) AS avis_trimestre,
    LAG(SUM(f.review_count)) OVER (
        PARTITION BY l.state
        ORDER BY c.year, c.quarter
    ) AS avis_trimestre_precedent,
    ROUND(
        (SUM(f.review_count) - LAG(SUM(f.review_count)) OVER (
            PARTITION BY l.state ORDER BY c.year, c.quarter)
        ) / NULLIF(LAG(SUM(f.review_count)) OVER (
            PARTITION BY l.state ORDER BY c.year, c.quarter), 0) * 100,
    2) AS croissance_pct
FROM FACT_PERFORMANCE f
JOIN DIM_LOCATION l ON f.location_id = l.location_id
JOIN DIM_CALENDAR c ON f.time_id     = c.ID_Temps
GROUP BY l.state, c.year, c.quarter
ORDER BY l.state, c.year, c.quarter;


-- ============================================================
-- Q6 : CROISSANCE ANNUELLE - Top 50 des commerces avec la plus
--      forte croissance d'avis d'une annee sur l'autre (YoY)
-- ============================================================
SELECT *
FROM (
    SELECT
        business_id,
        city,
        state,
        annee,
        avis_annee,
        avis_annee_precedente,
        ROUND(
            (avis_annee - avis_annee_precedente)
            / NULLIF(avis_annee_precedente, 0) * 100,
        2) AS croissance_yoy_pct
    FROM (
        SELECT
            f.business_id,
            l.city,
            l.state,
            c.year                                                        AS annee,
            SUM(f.review_count)                                           AS avis_annee,
            LAG(SUM(f.review_count)) OVER (
                PARTITION BY f.business_id ORDER BY c.year
            )                                                             AS avis_annee_precedente
        FROM FACT_PERFORMANCE f
        JOIN DIM_LOCATION l ON f.location_id = l.location_id
        JOIN DIM_CALENDAR c ON f.time_id     = c.ID_Temps
        GROUP BY f.business_id, l.city, l.state, c.year
    )
    WHERE avis_annee_precedente IS NOT NULL
      AND avis_annee_precedente > 0
)
ORDER BY croissance_yoy_pct DESC
FETCH FIRST 50 ROWS ONLY;


-- ============================================================
-- Q7 : MEILLEURES CATEGORIES par etat (top 5 par etat)
--      sur la base de la note moyenne et du volume d'avis
-- ============================================================
SELECT *
FROM (
    SELECT
        l.state,
        cat.category_name,
        COUNT(DISTINCT f.business_id)                                         AS nb_commerces,
        SUM(f.review_count)                                                    AS total_avis,
        ROUND(AVG(f.score_mean), 2)                                            AS note_moyenne,
        RANK() OVER (PARTITION BY l.state ORDER BY AVG(f.score_mean) DESC)    AS rang_dans_etat
    FROM FACT_PERFORMANCE f
    JOIN DIM_LOCATION          l   ON f.location_id  = l.location_id
    JOIN BRIDGE_BUSINESS_CATEGORY bbc ON f.business_id = bbc.business_id
    JOIN DIM_CATEGORY          cat ON bbc.category_id  = cat.category_id
    WHERE f.score_mean IS NOT NULL
    GROUP BY l.state, cat.category_name
)
WHERE rang_dans_etat <= 5
ORDER BY state, rang_dans_etat;


-- ============================================================
-- Q8 : SCORE DE POTENTIEL - Formule composite d'investissement
--      Score = note(35%) + sous-exposition(30%) + engagement(20%) + tips(15%)
--      Pepite = note >= 4.2 ET avis < 150 ET score >= 60
-- ============================================================
SELECT
    f.business_id,
    l.city,
    l.state,
    total_avis,
    total_checkins,
    total_tips,
    note_moyenne,
    ROUND(
        (note_moyenne / 5.0)                            * 35
        + (1 - LEAST(1, total_avis / 150.0))            * 30
        + LEAST(1, total_checkins / 100.0)              * 20
        + LEAST(1, total_tips / 20.0)                   * 15,
    1) AS score_potentiel,
    CASE
        WHEN note_moyenne >= 4.2
         AND total_avis < 150
         AND ROUND(
                (note_moyenne / 5.0)         * 35
                + (1 - LEAST(1, total_avis / 150.0)) * 30
                + LEAST(1, total_checkins / 100.0)   * 20
                + LEAST(1, total_tips / 20.0)         * 15, 1) >= 60
        THEN 'OUI'
        ELSE 'NON'
    END AS est_pepite
FROM (
    SELECT
        f.business_id,
        f.location_id,
        SUM(f.review_count)         AS total_avis,
        SUM(f.checkin_count)        AS total_checkins,
        SUM(f.tip_count)            AS total_tips,
        ROUND(AVG(f.score_mean), 2) AS note_moyenne
    FROM FACT_PERFORMANCE f
    WHERE f.score_mean IS NOT NULL
    GROUP BY f.business_id, f.location_id
) f
JOIN DIM_LOCATION l ON f.location_id = l.location_id
ORDER BY score_potentiel DESC;
