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

// Función para obtener el contacto asociado al negocio
async function obtenerContactoDelNegocio(objectId, token) {
  try {
    const response = await axios.get(
      `https://api.hubapi.com/crm/v3/objects/deals/${objectId}/associations/contacts`,
      {
        headers: {
          Authorization: `Bearer ${token}`
        }
      }
    );
    
    // Retorna el primer contacto asociado (o null si no hay)
    return response.data.results?.[0]?.id || null;
  } catch (error) {
    console.error(`Error obteniendo contacto del negocio ${objectId}:`, error.message);
    return null;
  }
}

// Función para verificar si el contacto ya recibió encuesta hoy
async function contactoRecibiEncuestaHoy(contactId, token, fechaControl) {
  try {
    const response = await axios.get(
      `https://api.hubapi.com/crm/v3/objects/contacts/${contactId}?properties=fechaMail`,
      {
        headers: {
          Authorization: `Bearer ${token}`
        }
      }
    );
    
    const fechaMail = response.data.properties?.fechaMail;
    
    if (!fechaMail) {
      return false; // No tiene fecha registrada, nunca ha recibido encuesta
    }
    
    // Convertir fechaMail a formato de fecha
    const fechaMailDate = new Date(fechaMail);
    const fechaMailControl = fechaMailDate.toISOString().split('T')[0];
    
    // Comparar si es la misma fecha
    return fechaMailControl === fechaControl;
    
  } catch (error) {
    console.error(`Error verificando fechaMail del contacto ${contactId}:`, error.message);
    return false; // En caso de error, permitir el envío
  }
}

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
        console.log(`Negocio ${objectId} ya existe en la base de datos. Ignorando duplicado.`);
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
          headers: {
            Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}`
          }
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
        console.warn(`Negocio ${objectId} sin concepto definido aún. Ignorando.`);
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

      // NUEVA VALIDACIÓN: Obtener el contacto asociado al negocio
      const contactId = await obtenerContactoDelNegocio(objectId, process.env.HUBSPOT_TOKEN);
      
      if (!contactId) {
        console.warn(`Negocio ${objectId} no tiene contacto asociado. Ignorando.`);
        resultados.push({ objectId, concepto, status: 'sin_contacto_asociado' });
        conn.release();
        continue;
      }

      // NUEVA VALIDACIÓN: Verificar si el contacto ya recibió encuesta hoy
      const yaRecibiEncuestaHoy = await contactoRecibiEncuestaHoy(contactId, process.env.HUBSPOT_TOKEN, fechaControl);
      
      if (yaRecibiEncuestaHoy) {
        console.log(`Contacto ${contactId} ya recibió encuesta hoy (${fechaControl}). Negocio ${objectId} marcado como NO enviar.`);
        
        // Registrar en BD pero marcar como NO enviar
        await conn.execute(
          `INSERT INTO registros (id, concepto, enviar_encuesta, fecha_creacion, fecha_cierre, contacto_id, razon_no_envio) 
           VALUES (?, ?, 0, NOW(), ?, ?, 'contacto_ya_recibio_encuesta_hoy')`,
          [objectId, concepto, fechaCierre, contactId]
        );

        // Actualizar HubSpot con NO
        await axios.patch(
          `https://api.hubapi.com/crm/v3/objects/deals/${objectId}`,
          {
            properties: {
              enviar_encuesta: 'NO'
            }
          },
          {
            headers: {
              Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}`,
              'Content-Type': 'application/json'
            }
          }
        );

        resultados.push({ 
          objectId, 
          concepto, 
          contacto_id: contactId,
          enviar_encuesta: false, 
          razon: 'contacto_ya_recibio_encuesta_hoy',
          fechaControl 
        });
        
        conn.release();
        continue;
      }

      // Validación del límite del concepto (lógica original)
      const [rows] = await conn.execute(
        `SELECT COUNT(*) as total FROM registros 
         WHERE concepto = ? AND enviar_encuesta = 1 
         AND DATE(fecha_cierre) = ?`,
        [concepto, fechaControl]
      );

      const usadosHoy = rows[0].total;
      const enviarEncuesta = usadosHoy < limite;
      const enviarEncuestaFlag = enviarEncuesta ? 1 : 0;

      try {
        await conn.execute(
          `INSERT INTO registros (id, concepto, enviar_encuesta, fecha_creacion, fecha_cierre, contacto_id) 
           VALUES (?, ?, ?, NOW(), ?, ?)`,
          [objectId, concepto, enviarEncuestaFlag, fechaCierre, contactId]
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

        console.log(`Negocio ${objectId} (Contacto: ${contactId}) actualizado → ${enviarEncuesta ? 'SI' : 'NO'}`);
        resultados.push({ 
          objectId, 
          concepto, 
          contacto_id: contactId,
          enviar_encuesta: enviarEncuesta, 
          fechaControl 
        });

      } catch (insertError) {
        if (insertError.code === 'ER_DUP_ENTRY') {
          console.log(`Negocio ${objectId} - Error de duplicado en INSERT. Ignorando.`);
          resultados.push({ objectId, status: 'duplicado_en_insert' });
        } else {
          throw insertError;
        }
      }

    } catch (err) {
      const status = err.response?.status;
      if (status === 404) {
        console.warn(`Negocio ${objectId} no encontrado (404). Puede haber sido eliminado`);
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
