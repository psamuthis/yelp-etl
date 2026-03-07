package wululu

import com.typesafe.config.ConfigFactory
import java.sql.DriverManager

object OraclePerformanceEval {

    private val config   = ConfigFactory.load("oracle_sink.conf")
    private val dbConfig = config.getConfig("oracle_sink")

    private val queries = List(
        ("Q1 ROLLUP état×année",
         """SELECT l.STATE, c.YEAR, COUNT(DISTINCT f.BUSINESS_ID), SUM(f.REVIEW_COUNT), ROUND(AVG(f.SCORE_MEAN),2)
            FROM FACT_PERFORMANCE f
            JOIN DIM_LOCATION l ON f.LOCATION_ID = l.LOCATION_ID
            JOIN DIM_CALENDAR c ON f.TIME_ID = c.ID_TEMPS
            WHERE f.SCORE_MEAN IS NOT NULL
            GROUP BY ROLLUP(l.STATE, c.YEAR)"""),

        ("Q3 RANK villes par note",
         """SELECT l.CITY, l.STATE, ROUND(AVG(f.SCORE_MEAN),2),
                   RANK() OVER (PARTITION BY l.STATE ORDER BY AVG(f.SCORE_MEAN) DESC)
            FROM FACT_PERFORMANCE f
            JOIN DIM_LOCATION l ON f.LOCATION_ID = l.LOCATION_ID
            WHERE f.SCORE_MEAN IS NOT NULL
            GROUP BY l.CITY, l.STATE"""),

        ("Q4 TOP-K pépites",
         """SELECT * FROM (
                SELECT f.BUSINESS_ID, l.CITY, l.STATE, SUM(f.REVIEW_COUNT), ROUND(AVG(f.SCORE_MEAN),2),
                       RANK() OVER (ORDER BY AVG(f.SCORE_MEAN) DESC, SUM(f.REVIEW_COUNT) ASC)
                FROM FACT_PERFORMANCE f
                JOIN DIM_LOCATION l ON f.LOCATION_ID = l.LOCATION_ID
                WHERE f.SCORE_MEAN IS NOT NULL
                GROUP BY f.BUSINESS_ID, l.CITY, l.STATE
                HAVING AVG(f.SCORE_MEAN) >= 4.2 AND SUM(f.REVIEW_COUNT) < 150
            ) WHERE ROWNUM <= 20"""),

        ("Q5 LAG trimestriel (AZ)",
         """SELECT c.YEAR, c.QUARTER, SUM(f.REVIEW_COUNT),
                   LAG(SUM(f.REVIEW_COUNT)) OVER (ORDER BY c.YEAR, c.QUARTER)
            FROM FACT_PERFORMANCE f
            JOIN DIM_LOCATION l ON f.LOCATION_ID = l.LOCATION_ID
            JOIN DIM_CALENDAR c ON f.TIME_ID = c.ID_TEMPS
            WHERE l.STATE = 'AZ'
            GROUP BY c.YEAR, c.QUARTER
            ORDER BY c.YEAR, c.QUARTER"""),

        ("Q7 Top catégories par état",
         """SELECT * FROM (
                SELECT l.STATE, cat.CATEGORY_NAME, COUNT(DISTINCT f.BUSINESS_ID),
                       ROUND(AVG(f.SCORE_MEAN),2),
                       RANK() OVER (PARTITION BY l.STATE ORDER BY AVG(f.SCORE_MEAN) DESC)
                FROM FACT_PERFORMANCE f
                JOIN DIM_LOCATION l ON f.LOCATION_ID = l.LOCATION_ID
                JOIN BRIDGE_BUSINESS_CATEGORY bbc ON f.BUSINESS_ID = bbc.BUSINESS_ID
                JOIN DIM_CATEGORY cat ON bbc.CATEGORY_ID = cat.CATEGORY_ID
                WHERE f.SCORE_MEAN IS NOT NULL
                GROUP BY l.STATE, cat.CATEGORY_NAME
            ) WHERE ROWNUM <= 100"""),

        ("Q8 Score potentiel (top 100)",
         """SELECT f.BUSINESS_ID, l.CITY, l.STATE, total_avis, total_checkins, note_moyenne,
                   ROUND((note_moyenne/5.0)*35 + (1-LEAST(1,total_avis/150.0))*30
                         + LEAST(1,total_checkins/100.0)*20, 1) AS SCORE
            FROM (
                SELECT f.BUSINESS_ID, f.LOCATION_ID,
                       SUM(f.REVIEW_COUNT) AS total_avis,
                       SUM(f.CHECKIN_COUNT) AS total_checkins,
                       ROUND(AVG(f.SCORE_MEAN),2) AS note_moyenne
                FROM FACT_PERFORMANCE f WHERE f.SCORE_MEAN IS NOT NULL
                GROUP BY f.BUSINESS_ID, f.LOCATION_ID
            ) f
            JOIN DIM_LOCATION l ON f.LOCATION_ID = l.LOCATION_ID
            ORDER BY SCORE DESC FETCH FIRST 100 ROWS ONLY"""),
    )

    private val indexes = List(
        "IDX_FACT_BUSINESS_ID", "IDX_FACT_TIME_ID", "IDX_FACT_LOCATION_ID",
        "IDX_FACT_BIZ_TIME", "IDX_FACT_LOC_TIME",
        "IDX_LOC_STATE", "IDX_LOC_BIZ_ID",
        "IDX_BIZ_STARS", "IDX_BIZ_STARS_OPEN",
        "IDX_BRIDGE_BIZ", "IDX_BRIDGE_CAT"
    )

    def timeQuery(stmt: java.sql.Statement, sql: String): Long = {
        val t0 = System.currentTimeMillis()
        val rs = stmt.executeQuery(sql)
        while (rs.next()) {}
        rs.close()
        System.currentTimeMillis() - t0
    }

    def main(args: Array[String]): Unit = {
        Class.forName(dbConfig.getString("driver"))
        val conn = DriverManager.getConnection(
            dbConfig.getString("url"),
            dbConfig.getString("user"),
            dbConfig.getString("password")
        )
        conn.setAutoCommit(true)
        val stmt = conn.createStatement()

        println("=" * 70)
        println("ÉVALUATION DES PERFORMANCES — Yelp DW Oracle")
        println("=" * 70)

        // ── MESURE AVEC INDEX ──────────────────────────────────────────────────
        println("\n>>> Phase 1 : WITH INDEX (état actuel)\n")
        val timesAvec = queries.map { case (name, sql) =>
            // warm-up
            try { timeQuery(stmt, sql) } catch { case _: Exception => 0L }
            val t = try { timeQuery(stmt, sql) } catch { case e: Exception => { println(s"  ERR $name: ${e.getMessage.take(60)}"); -1L } }
            println(f"  [$name] $t ms")
            (name, t)
        }

        // ── SUPPRESSION DES INDEX ──────────────────────────────────────────────
        println("\n>>> Suppression des index...")
        for (idx <- indexes) {
            try { stmt.execute(s"DROP INDEX $idx"); print(s"  -$idx ") }
            catch { case _: Exception => }
        }
        println()

        // ── MESURE SANS INDEX ──────────────────────────────────────────────────
        println("\n>>> Phase 2 : SANS INDEX\n")
        val typesSans = queries.map { case (name, sql) =>
            val t = try { timeQuery(stmt, sql) } catch { case e: Exception => { println(s"  ERR $name: ${e.getMessage.take(60)}"); -1L } }
            println(f"  [$name] $t ms")
            (name, t)
        }

        // ── RECRÉATION DES INDEX ───────────────────────────────────────────────
        println("\n>>> Recréation des index...")
        val ddls = Map(
            "IDX_FACT_BUSINESS_ID" -> "CREATE INDEX IDX_FACT_BUSINESS_ID  ON FACT_PERFORMANCE(BUSINESS_ID)",
            "IDX_FACT_TIME_ID"     -> "CREATE INDEX IDX_FACT_TIME_ID      ON FACT_PERFORMANCE(TIME_ID)",
            "IDX_FACT_LOCATION_ID" -> "CREATE INDEX IDX_FACT_LOCATION_ID  ON FACT_PERFORMANCE(LOCATION_ID)",
            "IDX_FACT_BIZ_TIME"    -> "CREATE INDEX IDX_FACT_BIZ_TIME     ON FACT_PERFORMANCE(BUSINESS_ID, TIME_ID)",
            "IDX_FACT_LOC_TIME"    -> "CREATE INDEX IDX_FACT_LOC_TIME     ON FACT_PERFORMANCE(LOCATION_ID, TIME_ID)",
            "IDX_LOC_STATE"        -> "CREATE INDEX IDX_LOC_STATE         ON DIM_LOCATION(STATE)",
            "IDX_LOC_BIZ_ID"      -> "CREATE INDEX IDX_LOC_BIZ_ID        ON DIM_LOCATION(BUSINESS_ID)",
            "IDX_BIZ_STARS"        -> "CREATE INDEX IDX_BIZ_STARS         ON DIM_BUSINESS(STARS)",
            "IDX_BIZ_STARS_OPEN"   -> "CREATE INDEX IDX_BIZ_STARS_OPEN    ON DIM_BUSINESS(STARS, IS_OPEN)",
            "IDX_BRIDGE_BIZ"       -> "CREATE INDEX IDX_BRIDGE_BIZ        ON BRIDGE_BUSINESS_CATEGORY(BUSINESS_ID)",
            "IDX_BRIDGE_CAT"       -> "CREATE INDEX IDX_BRIDGE_CAT        ON BRIDGE_BUSINESS_CATEGORY(CATEGORY_ID)",
        )
        for ((name, sql) <- ddls) {
            try { stmt.execute(sql); print(s"  +$name ") }
            catch { case e: Exception => println(s"  ERR $name: ${e.getMessage.take(40)}") }
        }
        println()

        // ── TABLEAU COMPARATIF ─────────────────────────────────────────────────
        println("\n" + "=" * 70)
        println("RÉSULTATS COMPARATIFS")
        println("=" * 70)
        println(f"${"Requête"}%-35s ${"Sans index"}%12s ${"Avec index"}%12s ${"Gain"}%8s")
        println("-" * 70)

        for (((name, tAvec), (_, tSans)) <- timesAvec.zip(typesSans)) {
            val gain = if (tAvec > 0 && tSans > 0) f"${(tSans - tAvec) * 100.0 / tSans}%.1f%%" else "N/A"
            println(f"${name}%-35s ${tSans}%10d ms ${tAvec}%10d ms ${gain}%8s")
        }

        println("=" * 70)

        stmt.close()
        conn.close()
    }
}
