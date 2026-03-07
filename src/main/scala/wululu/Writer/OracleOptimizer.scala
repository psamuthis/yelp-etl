package wululu

import com.typesafe.config.ConfigFactory
import java.sql.DriverManager

object OracleOptimizer {

    private val config   = ConfigFactory.load("oracle_sink.conf")
    private val dbConfig = config.getConfig("oracle_sink")

    def main(args: Array[String]): Unit = {
        Class.forName(dbConfig.getString("driver"))
        val conn = DriverManager.getConnection(
            dbConfig.getString("url"),
            dbConfig.getString("user"),
            dbConfig.getString("password")
        )
        conn.setAutoCommit(true)
        val stmt = conn.createStatement()

        val ddls = List(
            // ── INDEX sur les FK de FACT_PERFORMANCE ──────────────────────────────
            ("IDX_FACT_BUSINESS_ID",   "CREATE INDEX IDX_FACT_BUSINESS_ID  ON FACT_PERFORMANCE(BUSINESS_ID)"),
            ("IDX_FACT_TIME_ID",       "CREATE INDEX IDX_FACT_TIME_ID      ON FACT_PERFORMANCE(TIME_ID)"),
            ("IDX_FACT_LOCATION_ID",   "CREATE INDEX IDX_FACT_LOCATION_ID  ON FACT_PERFORMANCE(LOCATION_ID)"),
            // ── INDEX composites pour les requêtes OLAP fréquentes ─────────────────
            ("IDX_FACT_BIZ_TIME",      "CREATE INDEX IDX_FACT_BIZ_TIME     ON FACT_PERFORMANCE(BUSINESS_ID, TIME_ID)"),
            ("IDX_FACT_LOC_TIME",      "CREATE INDEX IDX_FACT_LOC_TIME     ON FACT_PERFORMANCE(LOCATION_ID, TIME_ID)"),
            // ── INDEX sur DIM_LOCATION pour les filtres géographiques ──────────────
            ("IDX_LOC_STATE",          "CREATE INDEX IDX_LOC_STATE         ON DIM_LOCATION(STATE)"),
            ("IDX_LOC_CITY_STATE",     "CREATE INDEX IDX_LOC_CITY_STATE    ON DIM_LOCATION(CITY, STATE)"),
            ("IDX_LOC_BIZ_ID",        "CREATE INDEX IDX_LOC_BIZ_ID        ON DIM_LOCATION(BUSINESS_ID)"),
            // ── INDEX sur DIM_BUSINESS pour les filtres de pépites ────────────────
            ("IDX_BIZ_STARS",          "CREATE INDEX IDX_BIZ_STARS         ON DIM_BUSINESS(STARS)"),
            ("IDX_BIZ_STARS_OPEN",     "CREATE INDEX IDX_BIZ_STARS_OPEN    ON DIM_BUSINESS(STARS, IS_OPEN)"),
            // ── INDEX sur BRIDGE pour les jointures catégorie ──────────────────────
            ("IDX_BRIDGE_BIZ",         "CREATE INDEX IDX_BRIDGE_BIZ        ON BRIDGE_BUSINESS_CATEGORY(BUSINESS_ID)"),
            ("IDX_BRIDGE_CAT",         "CREATE INDEX IDX_BRIDGE_CAT        ON BRIDGE_BUSINESS_CATEGORY(CATEGORY_ID)"),
        )

        println("=== Création des INDEX ===")
        for ((name, sql) <- ddls) {
            try {
                // Supprimer l'index s'il existe déjà
                try { stmt.execute(s"DROP INDEX $name") } catch { case _: Exception => }
                stmt.execute(sql)
                println(s"  [OK] $name")
            } catch {
                case e: Exception => println(s"  [ERR] $name : ${e.getMessage}")
            }
        }

        // ── VUES MATÉRIALISÉES ─────────────────────────────────────────────────────
        val mvs = List(
            ("MV_PERF_BY_STATE_YEAR",
             """CREATE MATERIALIZED VIEW MV_PERF_BY_STATE_YEAR
                BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
                SELECT l.STATE, c.YEAR,
                       COUNT(DISTINCT f.BUSINESS_ID) AS NB_COMMERCES,
                       SUM(f.REVIEW_COUNT)            AS TOTAL_REVIEWS,
                       SUM(f.CHECKIN_COUNT)           AS TOTAL_CHECKINS,
                       ROUND(AVG(f.SCORE_MEAN), 2)    AS NOTE_MOYENNE
                FROM FACT_PERFORMANCE f
                JOIN DIM_LOCATION l ON f.LOCATION_ID = l.LOCATION_ID
                JOIN DIM_CALENDAR c ON f.TIME_ID      = c.ID_TEMPS
                GROUP BY l.STATE, c.YEAR"""),

            ("MV_PEPITES_BY_STATE",
             """CREATE MATERIALIZED VIEW MV_PEPITES_BY_STATE
                BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
                SELECT l.STATE, l.CITY,
                       COUNT(DISTINCT b.BUSINESS_ID) AS NB_PEPITES,
                       ROUND(AVG(b.STARS), 2)        AS NOTE_MOYENNE
                FROM DIM_BUSINESS b
                JOIN DIM_LOCATION l      ON b.BUSINESS_ID = l.BUSINESS_ID
                JOIN FACT_PERFORMANCE f  ON b.BUSINESS_ID = f.BUSINESS_ID
                WHERE b.STARS >= 4.2 AND b.IS_OPEN = 1
                GROUP BY l.STATE, l.CITY
                HAVING SUM(f.REVIEW_COUNT) < 150 * COUNT(DISTINCT b.BUSINESS_ID)"""),

            ("MV_CATEGORY_STATS",
             """CREATE MATERIALIZED VIEW MV_CATEGORY_STATS
                BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND AS
                SELECT cat.CATEGORY_NAME,
                       COUNT(DISTINCT b.BUSINESS_ID) AS NB_COMMERCES,
                       ROUND(AVG(b.STARS), 2)        AS NOTE_MOYENNE,
                       SUM(f.REVIEW_COUNT)            AS TOTAL_REVIEWS
                FROM DIM_CATEGORY cat
                JOIN BRIDGE_BUSINESS_CATEGORY bc ON cat.CATEGORY_ID  = bc.CATEGORY_ID
                JOIN DIM_BUSINESS b              ON bc.BUSINESS_ID   = b.BUSINESS_ID
                JOIN FACT_PERFORMANCE f          ON b.BUSINESS_ID    = f.BUSINESS_ID
                GROUP BY cat.CATEGORY_NAME"""),
        )

        println("\n=== Création des VUES MATÉRIALISÉES ===")
        for ((name, sql) <- mvs) {
            try {
                try { stmt.execute(s"DROP MATERIALIZED VIEW $name") } catch { case _: Exception => }
                stmt.execute(sql)
                println(s"  [OK] $name")
            } catch {
                case e: Exception => println(s"  [ERR] $name : ${e.getMessage}")
            }
        }

        stmt.close()
        conn.close()
        println("\nOptimisation Oracle terminée.")
    }
}
