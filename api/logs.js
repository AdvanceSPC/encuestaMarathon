const mysql = require('mysql2/promise');
require('dotenv').config();

module.exports = async (req, res) => {
  try {
    const conn = await mysql.createConnection({
      host: process.env.DB_HOST,
      user: process.env.DB_USER,
      password: process.env.DB_PASS,
      database: process.env.DB_NAME
    });

    const [rows] = await conn.execute(
      `SELECT concepto, cantidad_actual, limite 
       FROM concepto_logs WHERE fecha_log = CURDATE()`
    );

    await conn.end();
    res.json(rows);
  } catch (err) {
    console.error('Error al obtener logs:', err);
    res.status(500).json({ error: 'Error del servidor' });
  }
};
