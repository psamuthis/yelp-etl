package wululu

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.jdbc.JdbcDialects

import wululu.SparkSessionWrapper.spark

object QueryRunner {

    private val config   = ConfigFactory.load("oracle_sink.conf")
    private val dbConfig = config.getConfig("oracle_sink")
    private val jdbcUrl  = dbConfig.getString("url")

    private val connectionProperties = {
        val props = new java.util.Properties()
        props.setProperty("user",     dbConfig.getString("user"))
        props.setProperty("password", dbConfig.getString("password"))
        props.setProperty("driver",   dbConfig.getString("driver"))
        props
    }

    def runQuery(label: String, sql: String, limit: Int = 20): Unit = {
        println(s"\n${"=" * 60}")
        println(s"$label")
        println("=" * 60)
        val df = spark.read.jdbc(jdbcUrl, s"($sql)", connectionProperties)
        df.show(limit, truncate = false)
        println(s"=> ${df.count()} lignes")
    }

    def main(args: Array[String]): Unit = {
        JdbcDialects.registerDialect(new OracleDialect())

        // Q1 : ROLLUP - Avis et notes par etat/annee
        runQuery("Q1 - ROLLUP : Avis et notes par etat > annee",
            """SELECT NVL(l.STATE, 'TOTAL_GLOBAL') AS ETAT,
                      NVL(TO_CHAR(c.YEAR), 'TOTAL_ANNEE') AS ANNEE,
                      COUNT(DISTINCT f.BUSINESS_ID) AS NB_COMMERCES,
                      SUM(f.REVIEW_COUNT) AS TOTAL_AVIS,
                      SUM(f.CHECKIN_COUNT) AS TOTAL_CHECKINS,
                      ROUND(AVG(f.SCORE_MEAN), 2) AS NOTE_MOYENNE
               FROM FACT_PERFORMANCE f
               JOIN DIM_LOCATION l ON f.LOCATION_ID = l.LOCATION_ID
               JOIN DIM_CALENDAR c ON f.TIME_ID = c.ID_TEMPS
               WHERE f.SCORE_MEAN IS NOT NULL
               GROUP BY ROLLUP(l.STATE, c.YEAR)
               ORDER BY l.STATE NULLS LAST, c.YEAR NULLS LAST""")

        // Q3 : RANK - Classement des villes par note moyenne
        runQuery("Q3 - RANK : Top 30 villes par note moyenne",
            """SELECT * FROM (
                 SELECT l.CITY, l.STATE,
                        COUNT(DISTINCT f.BUSINESS_ID) AS NB_COMMERCES,
                        SUM(f.REVIEW_COUNT) AS TOTAL_AVIS,
                        ROUND(AVG(f.SCORE_MEAN), 2) AS NOTE_MOYENNE,
                        RANK() OVER (PARTITION BY l.STATE ORDER BY AVG(f.SCORE_MEAN) DESC) AS RANG_ETAT,
                        DENSE_RANK() OVER (ORDER BY AVG(f.SCORE_MEAN) DESC) AS RANG_GLOBAL
                 FROM FACT_PERFORMANCE f
                 JOIN DIM_LOCATION l ON f.LOCATION_ID = l.LOCATION_ID
                 WHERE f.SCORE_MEAN IS NOT NULL
                 GROUP BY l.CITY, l.STATE
               )
               WHERE RANG_GLOBAL <= 30
               ORDER BY RANG_GLOBAL""", 30)

        // Q4 : TOP-K - Top 20 pepites
        runQuery("Q4 - TOP-K : Top 20 pepites (note >= 4.2, avis < 150)",
            """SELECT * FROM (
                 SELECT f.BUSINESS_ID, l.CITY, l.STATE,
                        SUM(f.REVIEW_COUNT) AS TOTAL_AVIS,
                        SUM(f.CHECKIN_COUNT) AS TOTAL_CHECKINS,
                        SUM(f.TIP_COUNT) AS TOTAL_TIPS,
                        ROUND(AVG(f.SCORE_MEAN), 2) AS NOTE_MOYENNE,
                        RANK() OVER (ORDER BY AVG(f.SCORE_MEAN) DESC, SUM(f.REVIEW_COUNT) ASC) AS RANG
                 FROM FACT_PERFORMANCE f
                 JOIN DIM_LOCATION l ON f.LOCATION_ID = l.LOCATION_ID
                 WHERE f.SCORE_MEAN IS NOT NULL
                 GROUP BY f.BUSINESS_ID, l.CITY, l.STATE
                 HAVING AVG(f.SCORE_MEAN) >= 4.2 AND SUM(f.REVIEW_COUNT) < 150
               )
               WHERE RANG <= 20
               ORDER BY RANG""")

        // Q5 : FENETRAGE - Evolution trimestrielle par etat (top 5 etats)
        runQuery("Q5 - FENETRAGE : Evolution trimestrielle (AZ, CA, FL, NV, PA)",
            """SELECT STATE, YEAR, QUARTER, AVIS_TRIMESTRE, AVIS_TRIM_PRECEDENT,
                      CROISSANCE_PCT
               FROM (
                 SELECT l.STATE, c.YEAR, c.QUARTER,
                        SUM(f.REVIEW_COUNT) AS AVIS_TRIMESTRE,
                        LAG(SUM(f.REVIEW_COUNT)) OVER (
                            PARTITION BY l.STATE ORDER BY c.YEAR, c.QUARTER
                        ) AS AVIS_TRIM_PRECEDENT,
                        ROUND(
                            (SUM(f.REVIEW_COUNT) - LAG(SUM(f.REVIEW_COUNT)) OVER (
                                PARTITION BY l.STATE ORDER BY c.YEAR, c.QUARTER)
                            ) / NULLIF(LAG(SUM(f.REVIEW_COUNT)) OVER (
                                PARTITION BY l.STATE ORDER BY c.YEAR, c.QUARTER), 0) * 100, 2
                        ) AS CROISSANCE_PCT
                 FROM FACT_PERFORMANCE f
                 JOIN DIM_LOCATION l ON f.LOCATION_ID = l.LOCATION_ID
                 JOIN DIM_CALENDAR c ON f.TIME_ID = c.ID_TEMPS
                 WHERE l.STATE IN ('AZ','CA','FL','NV','PA')
                 GROUP BY l.STATE, c.YEAR, c.QUARTER
               )
               ORDER BY STATE, YEAR, QUARTER""", 50)

        // Q6 : CROISSANCE YoY - Top 20 commerces en plus forte croissance
        runQuery("Q6 - CROISSANCE YoY : Top 20 commerces en forte croissance",
            """SELECT * FROM (
                 SELECT BUSINESS_ID, CITY, STATE, ANNEE,
                        AVIS_ANNEE, AVIS_ANNEE_PREC,
                        ROUND((AVIS_ANNEE - AVIS_ANNEE_PREC)
                              / NULLIF(AVIS_ANNEE_PREC, 0) * 100, 2) AS CROISSANCE_YOY_PCT
                 FROM (
                   SELECT f.BUSINESS_ID, l.CITY, l.STATE, c.YEAR AS ANNEE,
                          SUM(f.REVIEW_COUNT) AS AVIS_ANNEE,
                          LAG(SUM(f.REVIEW_COUNT)) OVER (
                              PARTITION BY f.BUSINESS_ID ORDER BY c.YEAR
                          ) AS AVIS_ANNEE_PREC
                   FROM FACT_PERFORMANCE f
                   JOIN DIM_LOCATION l ON f.LOCATION_ID = l.LOCATION_ID
                   JOIN DIM_CALENDAR c ON f.TIME_ID = c.ID_TEMPS
                   GROUP BY f.BUSINESS_ID, l.CITY, l.STATE, c.YEAR
                 )
                 WHERE AVIS_ANNEE_PREC IS NOT NULL AND AVIS_ANNEE_PREC > 0
               )
               WHERE ROWNUM <= 20
               ORDER BY CROISSANCE_YOY_PCT DESC""")

        // Q7 : Top 5 categories par etat
        runQuery("Q7 - TOP CATEGORIES : Top 5 categories par etat",
            """SELECT * FROM (
                 SELECT l.STATE, cat.CATEGORY_NAME,
                        COUNT(DISTINCT f.BUSINESS_ID) AS NB_COMMERCES,
                        SUM(f.REVIEW_COUNT) AS TOTAL_AVIS,
                        ROUND(AVG(f.SCORE_MEAN), 2) AS NOTE_MOYENNE,
                        RANK() OVER (PARTITION BY l.STATE ORDER BY AVG(f.SCORE_MEAN) DESC) AS RANG_ETAT
                 FROM FACT_PERFORMANCE f
                 JOIN DIM_LOCATION l ON f.LOCATION_ID = l.LOCATION_ID
                 JOIN BRIDGE_BUSINESS_CATEGORY bbc ON f.BUSINESS_ID = bbc.BUSINESS_ID
                 JOIN DIM_CATEGORY cat ON bbc.CATEGORY_ID = cat.CATEGORY_ID
                 WHERE f.SCORE_MEAN IS NOT NULL
                 GROUP BY l.STATE, cat.CATEGORY_NAME
               )
               WHERE RANG_ETAT <= 5
               ORDER BY STATE, RANG_ETAT""", 50)

        // Q8 : Score de potentiel - Top 30 pepites
        runQuery("Q8 - SCORE POTENTIEL : Top 30 pepites avec score composite",
            """SELECT * FROM (
                 SELECT f.BUSINESS_ID, l.CITY, l.STATE,
                        TOTAL_AVIS, TOTAL_CHECKINS, TOTAL_TIPS, NOTE_MOYENNE,
                        ROUND(
                            (NOTE_MOYENNE / 5.0) * 35
                            + (1 - LEAST(1, TOTAL_AVIS / 150.0)) * 30
                            + LEAST(1, TOTAL_CHECKINS / 100.0) * 20
                            + LEAST(1, TOTAL_TIPS / 20.0) * 15, 1
                        ) AS SCORE_POTENTIEL,
                        CASE WHEN NOTE_MOYENNE >= 4.2 AND TOTAL_AVIS < 150 THEN 'OUI' ELSE 'NON' END AS EST_PEPITE
                 FROM (
                   SELECT f.BUSINESS_ID, f.LOCATION_ID,
                          SUM(f.REVIEW_COUNT) AS TOTAL_AVIS,
                          SUM(f.CHECKIN_COUNT) AS TOTAL_CHECKINS,
                          SUM(f.TIP_COUNT) AS TOTAL_TIPS,
                          ROUND(AVG(f.SCORE_MEAN), 2) AS NOTE_MOYENNE
                   FROM FACT_PERFORMANCE f
                   WHERE f.SCORE_MEAN IS NOT NULL
                   GROUP BY f.BUSINESS_ID, f.LOCATION_ID
                 ) f
                 JOIN DIM_LOCATION l ON f.LOCATION_ID = l.LOCATION_ID
               )
               WHERE EST_PEPITE = 'OUI'
               ORDER BY SCORE_POTENTIEL DESC""", 30)

        println("\nToutes les requêtes OLAP exécutées.")
    }
}
