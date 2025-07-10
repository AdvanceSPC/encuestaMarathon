const mysql = require('mysql2/promise');
const axios = require('axios');
require('dotenv').config();

const CONCEPT_LIMITS = {
    'MARATHON': 1000,
    'EXPLORER': 217,
    'BODEGA': 252,
    'OUTLET': 400,
    'TELESHOP': 197,
    'PUMA': 1300,
    'TAF': 1300,
    'UNDERARMOUR': 450
};

module.exports = async (req, res) => {
    try {
        const webhookData = Array.isArray(req.body) ? req.body[0] : req.body;
        const objectId = webhookData.objectId;

        console.log('Webhook recibido:', JSON.stringify(req.body, null, 2));
        console.log('Webhook data procesado:', JSON.stringify(objectId, null, 2));

        if (!objectId) {
            console.error('ObjectId no encontrado en el webhook');
            return res.status(400).json({
                error: 'Falta el ID del negocio (objectId)',
                receivedData: req.body,
                processedData: webhookData
            });
        }

        const hubspotRes = await axios.get(
            `https://api.hubapi.com/crm/v3/objects/deals/${objectId}?properties=concepto`,
            {
                headers: {
                    Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}`
                }
            }
        );

        const concepto = hubspotRes.data.properties?.concepto;
        if (!concepto || !CONCEPT_LIMITS[concepto.toUpperCase()]) {
            return res.status(400).json({
                error: 'Concepto no válido o no definido en el negocio',
                concepto: concepto,
                availableConcepts: Object.keys(CONCEPT_LIMITS)
            });
        }

        const limit = CONCEPT_LIMITS[concepto.toUpperCase()];

        const conn = await mysql.createConnection({
            host: process.env.DB_HOST,
            user: process.env.DB_USER,
            password: process.env.DB_PASS,
            database: process.env.DB_NAME
        });

        const [rows] = await conn.execute(
            `SELECT COUNT(*) as total FROM registros 
       WHERE concepto = ? AND enviar_encuesta = 1 
       AND DATE(fecha_creacion) = CURDATE()`,
            [concepto]
        );

        const usadosHoy = rows[0].total;
        const enviarEncuesta = usadosHoy < limit;

        await conn.execute(
            `INSERT INTO registros (id, concepto, enviar_encuesta, fecha_creacion) 
       VALUES (?, ?, ?, NOW())`,
            [objectId, concepto, enviarEncuesta ? 1 : 0]
        );

        await conn.execute(
            `INSERT INTO concepto_logs (concepto, cantidad_actual, limite, fecha_log)
       VALUES (?, 1, ?, CURDATE())
       ON DUPLICATE KEY UPDATE cantidad_actual = cantidad_actual + 1`,
            [concepto, limit]
        );

        await axios.patch(
            `https://api.hubapi.com/crm/v3/objects/deals/${objectId}`,
            {
                properties: {
                    enviar_encuesta: enviarEncuesta ? 'Sí' : 'No'
                }
            },
            {
                headers: {
                    Authorization: `Bearer ${process.env.HUBSPOT_TOKEN}`,
                    'Content-Type': 'application/json'
                }
            }
        );

        await conn.end();

        res.json({
            success: true,
            objectId,
            concepto,
            enviar_encuesta: enviarEncuesta,
            usados_hoy: usadosHoy,
            limite: limit
        });

    } catch (err) {
        console.error('Error en webhook:', err.response?.data || err.message);
        res.status(500).json({
            error: 'Error del servidor',
            detail: err.message,
            stack: err.stack
        });
    }
};