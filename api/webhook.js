const mysql = require('mysql2/promise');
const axios = require('axios');
require('dotenv').config();

const CONCEPT_LIMITS = {
  'MARATHON': 1000,
  'EXPLORER': 217,
  'BODEGAS DEPORTIVAS': 252,
  'OUTLET': 400,
  'TELESHOP': 197,
  'PUMA': 43,
  'TAF': 43,
  'TIENDA UNDER ARMOUR': 15
};

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

module.exports = async (req, res) => {
  const eventos = Array.isArray(req.body) ? req.body : [req.body];
  console.log('Webhook recibido:', JSON.stringify(eventos, null, 2));

  const resultados = [];

  for (const evento of eventos) {
    const objectId = evento.objectId;
    if (!objectId) continue;

    let conn;
    try {
      console.log(`Procesando negocio: ${objectId}`);
      conn = await pool.getConnection();

      const [existingRows] = await conn.execute(
        `SELECT id, concepto, enviar_encuesta, fecha_creacion FROM registros WHERE id = ?`,
        [objectId]
      );

      if (existingRows.length > 0) {
        console.log(`Negocio ${objectId} ya existe. Ignorando duplicado.`);
        resultados.push({ 
          objectId, 
          status: 'duplicado_ignorado',
          concepto: existingRows[0].concepto,
          enviar_encuesta: existingRows[0].enviar_encuesta === 1,
          fecha_creacion: existingRows[0].fecha_creacion
        });
        conn.release();
        continue;
      }

      const hubspotRes = await axios.get(
        `https://api.hubapi.com/crm/v3/objects/deals/${objectId}?properties=concepto,closedate`,
        {
          headers: { Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}` }
        }
      );

      const concepto = hubspotRes.data.properties?.concepto;
      const closedateRaw = hubspotRes.data.properties?.closedate;
      const fechaCierre = closedateRaw && !isNaN(Date.parse(closedateRaw))
        ? new Date(closedateRaw)
        : null;

      const fechaControl = fechaCierre
        ? fechaCierre.toISOString().split('T')[0]
        : new Date().toISOString().split('T')[0];

      if (!concepto) {
        console.warn(`Negocio ${objectId} sin concepto definido. Ignorando.`);
        resultados.push({ objectId, status: 'sin_concepto' });
        conn.release();
        continue;
      }

      const limite = CONCEPT_LIMITS[concepto.toUpperCase()];
      if (!limite) {
        console.warn(`Concepto no reconocido: ${concepto}`);
        resultados.push({ objectId, concepto, status: 'concepto_no_valido' });
        conn.release();
        continue;
      }

      const asociacionesRes = await axios.get(
        `https://api.hubapi.com/crm/v4/objects/deals/${objectId}/associations/contacts`,
        {
          headers: { Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}` }
        }
      );

      const contactoId = asociacionesRes.data.results?.[0]?.toObjectId;
      if (!contactoId) {
        console.warn(`Negocio ${objectId} sin contacto asociado. Ignorando.`);
        resultados.push({ objectId, status: 'sin_contacto' });
        conn.release();
        continue;
      }

      const [contactoEncuesta] = await conn.execute(
        `SELECT COUNT(*) as total 
         FROM registros 
         WHERE contact_id = ? 
           AND enviar_encuesta = 1 
           AND DATE(fecha_cierre) = ?`,
        [contactoId, fechaControl]
      );
      const yaEncuestadoHoy = contactoEncuesta[0].total > 0;

      const [rows] = await conn.execute(
        `SELECT COUNT(*) as total FROM registros 
         WHERE concepto = ? AND enviar_encuesta = 1 
         AND DATE(fecha_cierre) = ?`,
        [concepto, fechaControl]
      );

      const usadosHoy = rows[0].total;
      const enviarEncuesta = usadosHoy < limite && !yaEncuestadoHoy;
      const enviarEncuestaFlag = enviarEncuesta ? 1 : 0;

      try {
        await conn.execute(
          `INSERT INTO registros (id, contact_id, concepto, enviar_encuesta, fecha_creacion, fecha_cierre) 
           VALUES (?, ?, ?, ?, NOW(), ?)`,
          [objectId, contactoId, concepto, enviarEncuestaFlag, fechaCierre]
        );

        await conn.execute(
          `INSERT INTO concepto_logs (concepto, cantidad_actual, limite, fecha_log)
           VALUES (?, 1, ?, ?)
           ON DUPLICATE KEY UPDATE cantidad_actual = cantidad_actual + 1`,
          [concepto, limite, fechaControl]
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
        resultados.push({ 
          objectId, 
          contactoId, 
          concepto, 
          enviar_encuesta: enviarEncuesta, 
          fechaControl 
        });

      } catch (insertError) {
        if (insertError.code === 'ER_DUP_ENTRY') {
          console.log(`Negocio ${objectId} - Duplicado en INSERT. Ignorando.`);
          resultados.push({ objectId, status: 'duplicado_en_insert' });
        } else {
          throw insertError;
        }
      }

    } catch (err) {
      const status = err.response?.status;
      if (status === 404) {
        console.warn(`Negocio ${objectId} no encontrado (404).`);
        resultados.push({ objectId, status: 404, error: 'No encontrado en HubSpot' });
      } else {
        console.error(`Error al procesar ${objectId}:`, err.response?.data || err.message);
        resultados.push({ objectId, error: err.message });
      }
    } finally {
      if (conn) conn.release();
    }

    await sleep(8000);
  }

  res.status(200).json({
    processed: resultados.length,
    resultados
  });
};