const mysql = require('mysql2/promise');
const axios = require('axios');
require('dotenv').config();

const CONCEPT_LIMITS = {
  'MARATHON': 1000,
  'EXPLORER': 217,
  'BODEGAS DEPORTIVAS': 252,
  'OUTLET': 400,
  'TELESHOP': 197,
  'PUMA': 50,
  'TAF': 50,
  'TIENDA UNDER ARMOUR': 20
};

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

module.exports = async (req, res) => {
  const eventos = Array.isArray(req.body) ? req.body : [req.body];
  console.log('Webhook recibido:', JSON.stringify(eventos, null, 2));

  const conn = await mysql.createConnection({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    database: process.env.DB_NAME
  });

  const resultados = [];

  for (const evento of eventos) {
    const objectId = evento.objectId;
    if (!objectId) continue;

    try {
      console.log(`Procesando negocio: ${objectId}`);

      const hubspotRes = await axios.get(
        `https://api.hubapi.com/crm/v3/objects/deals/${objectId}?properties=concepto`,
        {
          headers: {
            Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}`
          }
        }
      );

      const concepto = hubspotRes.data.properties?.concepto;

      if (!concepto) {
        console.warn(`Negocio ${objectId} sin concepto aún. Ignorando.`);
        resultados.push({ objectId, status: 'sin_concepto' });
        continue;
      }

      const limite = CONCEPT_LIMITS[concepto.toUpperCase()];
      if (!limite) {
        console.warn(`Concepto no reconocido: ${concepto}`);
        resultados.push({ objectId, concepto, status: 'concepto_no_valido' });
        continue;
      }

      const [rows] = await conn.execute(
        `SELECT COUNT(*) as total FROM registros 
         WHERE concepto = ? AND enviar_encuesta = 1 
         AND DATE(fecha_creacion) = CURDATE()`,
        [concepto]
      );

      const usadosHoy = rows[0].total;
      const enviarEncuesta = usadosHoy < limite;

      await conn.execute(
        `INSERT INTO registros (id, concepto, enviar_encuesta, fecha_creacion) 
         VALUES (?, ?, ?, NOW())`,
        [objectId, concepto, enviarEncuesta ? 1 : 0]
      );

      await conn.execute(
        `INSERT INTO concepto_logs (concepto, cantidad_actual, limite, fecha_log)
         VALUES (?, 1, ?, CURDATE())
         ON DUPLICATE KEY UPDATE cantidad_actual = cantidad_actual + 1`,
        [concepto, limite]
      );

      await axios.patch(
        `https://api.hubapi.com/crm/v3/objects/deals/${objectId}`,
        {
          properties: {
            enviar_encuesta: enviarEncuesta ? 'SI' : 'NO'
          }
        },
        {
          headers: {
            Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}`,
            'Content-Type': 'application/json'
          }
        }
      );

      console.log(`Negocio ${objectId} actualizado → ${enviarEncuesta ? 'SI' : 'NO'}`);
      resultados.push({ objectId, concepto, enviar_encuesta: enviarEncuesta });

    } catch (err) {
      console.error(`Error al procesar ${objectId}:`, err.response?.data || err.message);
      resultados.push({ objectId, error: err.message });
    }

    await sleep(200);
  }

  await conn.end();

  res.status(200).json({
    processed: resultados.length,
    resultados
  });
};
