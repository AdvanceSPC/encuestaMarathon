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

const pool = mysql.createPool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function procesarNegocio(objectId) {
  let conn;
  try {
    conn = await pool.getConnection();

    const [exists] = await conn.execute(
      'SELECT id FROM registros WHERE id = ?',
      [objectId]
    );
    if (exists.length > 0) {
      return { objectId, status: 'duplicado_ignorado' };
    }

    const hubspotRes = await axios.get(
      `https://api.hubapi.com/crm/v3/objects/deals/${objectId}?properties=concepto,closedate`,
      { headers: { Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}` } }
    );

    const concepto = hubspotRes.data.properties?.concepto;
    const closedateRaw = hubspotRes.data.properties?.closedate;
    const fechaCierre = closedateRaw && !isNaN(Date.parse(closedateRaw))
      ? new Date(closedateRaw)
      : new Date();
    const fechaControl = fechaCierre.toISOString().split('T')[0];

    if (!concepto || !CONCEPT_LIMITS[concepto.toUpperCase()]) {
      return { objectId, status: 'concepto_no_valido' };
    }

    const limite = CONCEPT_LIMITS[concepto.toUpperCase()];

    const asociacionesRes = await axios.get(
      `https://api.hubapi.com/crm/v4/objects/deals/${objectId}/associations/contacts`,
      { headers: { Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}` } }
    );

    const contactoId = asociacionesRes.data.results?.[0]?.toObjectId;
    if (!contactoId) {
      return { objectId, status: 'sin_contacto' };
    }

    const [contactoEncuesta] = await conn.execute(
      `SELECT COUNT(*) as total FROM registros
       WHERE contacto_id = ? AND DATE(fecha_cierre) = ? AND enviar_encuesta = 1`,
      [contactoId, fechaControl]
    );
    const yaEncuestadoHoy = contactoEncuesta[0].total > 0;

    const [rows] = await conn.execute(
      `SELECT COUNT(*) as total FROM registros
       WHERE concepto = ? AND enviar_encuesta = 1 AND DATE(fecha_cierre) = ?`,
      [concepto, fechaControl]
    );

    const usadosHoy = rows[0].total;
    const enviarEncuesta = usadosHoy < limite && !yaEncuestadoHoy;

    await conn.execute(
      `INSERT INTO registros (id, contacto_id, concepto, enviar_encuesta, fecha_creacion, fecha_cierre)
       VALUES (?, ?, ?, ?, NOW(), ?)`,
      [objectId, contactoId, concepto, enviarEncuesta ? 1 : 0, fechaCierre]
    );

    await axios.patch(
      `https://api.hubapi.com/crm/v3/objects/deals/${objectId}`,
      { properties: { enviar_encuesta: enviarEncuesta ? 'SI' : 'NO' } },
      {
        headers: {
          Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}`,
          'Content-Type': 'application/json'
        }
      }
    );

    console.log(`✔ [${concepto}] Deal ${objectId} → ${enviarEncuesta ? 'SI' : 'NO'}`);
    return { objectId, contactoId, concepto, enviar_encuesta: enviarEncuesta };

  } catch (err) {
    console.error(`Error en negocio ${objectId}:`, err.message);
    return { objectId, error: err.message };
  } finally {
    if (conn) conn.release();
  }
}

function dividirEnBloques(arr, size) {
  const res = [];
  for (let i = 0; i < arr.length; i += size) {
    res.push(arr.slice(i, i + size));
  }
  return res;
}

module.exports = async (req, res) => {
  const eventos = Array.isArray(req.body) ? req.body : [req.body];
  console.log(`Recibidos ${eventos.length} eventos`);

  const resultados = [];
  const batchSize = 10;
  const bloques = dividirEnBloques(eventos, batchSize);

  for (const [i, bloque] of bloques.entries()) {
    console.log(`Procesando bloque ${i + 1}/${bloques.length} (${bloque.length} eventos)`);

    const promesas = bloque.map(e => procesarNegocio(e.objectId));
    const resultadosBloque = await Promise.allSettled(promesas);

    resultados.push(...resultadosBloque.map(r => r.value || r.reason));

    if (i < bloques.length - 1) {
      console.log(`Esperando antes del siguiente bloque...`);
      await sleep(8000);
    }
  }

  console.log(`✅ Procesamiento completado (${resultados.length} negocios)`);
  res.status(200).json({ processed: resultados.length, resultados });
};