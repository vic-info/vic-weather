const express = require("express");

const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/;

function createWeatherRouter(query) {
  const router = express.Router();

  router.get("/cities", async (req, res, next) => {
  try {
    const result = await query(
      `SELECT DISTINCT city
       FROM "default".weather_daily_metrics
       ORDER BY city`
    );
    res.json({ data: result.rows.map((row) => row.city) });
  } catch (error) {
    next(error);
  }
  });

  router.get("/daily", async (req, res, next) => {
  const { city, from, to } = req.query;

  if (!city || !from || !to) {
    return res.status(400).json({ error: "city, from and to are required" });
  }
  if (!ISO_DATE.test(from) || !ISO_DATE.test(to) || from > to) {
    return res.status(400).json({ error: "from and to must be valid YYYY-MM-DD values" });
  }

  try {
    const result = await query(
      `SELECT city,
              weather_date::date::text AS "weatherDate",
              max_temp_c AS "maxTempC",
              min_temp_c AS "minTempC",
              mean_temp_c AS "meanTempC",
              precipitation_mm AS "precipitationMm",
              rain_mm AS "rainMm",
              snowfall_cm AS "snowfallCm",
              max_wind_speed_kmh AS "maxWindSpeedKmh",
              is_rainy_day AS "isRainyDay",
              is_hot_day AS "isHotDay",
              is_freezing_day AS "isFreezingDay",
              weather_severity_score AS "weatherSeverityScore"
       FROM "default".weather_daily_metrics
       WHERE city = $1
         AND weather_date BETWEEN $2::date AND $3::date
       ORDER BY weather_date`,
      [city, from, to]
    );

    res.json({
      data: result.rows,
      meta: { city, from, to, count: result.rowCount },
    });
  } catch (error) {
    next(error);
  }
  });

  return router;
}

module.exports = { createWeatherRouter };
