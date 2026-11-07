// Propinas · Beraza y Cia — guardado en la nube (Vercel Function + Upstash Redis). Sin dependencias.
// Variables de entorno en Vercel:
//   KV_REST_API_URL / KV_REST_API_TOKEN  (las crea solas la base Upstash Redis al conectarla al proyecto)
//   CLAVE_ADMIN  contraseña del usuario ADMINISTRADOR (acceso total)
//   CLAVE_PLAYA  contraseña del usuario PLAYA (compartido: solo arma paquetes de efectivo)
//
// Las propinas se guardan como movimientos que nunca se borran (lista propinas:ev):
//   alta: { id, t:'alta', f, p, c, ts, por:'PLAYA'|'ADMIN', quien }
//   corr: { id, t:'corr', ref, f, p, pAntes, c, antes, ts, por:'ADMIN', quien, motivo }
// La fecha y hora las pone el servidor (hora de Argentina). PLAYA solo puede cargar en el día de hoy.
//
// Paquetes de efectivo armados (lista propinas:pq). Suman a la caja de propinas (no son pagos) y quedan
// pendientes hasta que el administrador los aprueba o los corrige.
//   paq: { id, t:'paq', n, f, turno:'M'|'T'|'N', ts, items:[{p,nombre,c}], total, por, quien }
//   obs: { id, t:'obs', ref, n, accion:'ok'|'corr'|'obs'|'anular', ts, quien, texto, cambios?:[{p,antes,c}] }
//   'anular': el paquete queda con su número (la numeración sigue correlativa) pero deja de sumar a la caja.
const crypto = require('crypto');

// Acepta cualquier prefijo que ponga Vercel al conectar la base (KV_, STORAGE_, etc.).
const pref = (() => { const env = process.env; if (env.KV_REST_API_URL && env.KV_REST_API_TOKEN) return 'KV'; if (env.UPSTASH_REDIS_REST_URL) return 'UPSTASH_REDIS';
  const k = Object.keys(env).find(x => /_REST_API_URL$/.test(x) && env[x.replace(/_URL$/, '_TOKEN')]); return k ? k.replace(/_REST_API_URL$/, '') : 'KV'; })();
const URL_R = (process.env[pref + '_REST_API_URL'] || process.env.UPSTASH_REDIS_REST_URL || '').replace(/\/$/, '');
const TOK_R = process.env[pref + '_REST_API_TOKEN'] || process.env.UPSTASH_REDIS_REST_TOKEN || '';
const CLAVES = { admin: process.env.CLAVE_ADMIN || '', playa: process.env.CLAVE_PLAYA || '' };
const K = { datos: 'propinas:datos', rev: 'propinas:rev', cambio: 'propinas:cambio', respHoy: 'propinas:respaldo:ultimo',
  emp: 'propinas:emp', ev: 'propinas:ev', ids: 'propinas:ids', c: 'propinas:c', tot: 'propinas:tot', pq: 'propinas:pq', pqseq: 'propinas:pqseq',
  pqdia: 'propinas:pqdia', pqdiaOk: 'propinas:pqdia:ok' }; // pqdia: total de paquetes por fecha (con correcciones y anulaciones) para el acumulado de PLAYA
const dia = f => 'propinas:dia:' + f, evd = f => 'propinas:evd:' + f;
/** Total vigente por fecha de los paquetes: aplica correcciones y anulaciones del administrador. */
function totalesPorDia(lst) {
  const paqs = {}, out = {};
  lst.forEach(x => { if (x.t === 'paq') { const m = {}; x.items.forEach(i => { m[i.p] = i.c; }); paqs[x.id] = { f: x.f, m }; } });
  lst.forEach(o => { const q = o.t === 'obs' && paqs[o.ref]; if (!q) return; if (o.p) q.m[o.p] = o.c; (o.cambios || []).forEach(c => { q.m[c.p] = c.c; }); if (o.accion === 'anular') Object.keys(q.m).forEach(p => { q.m[p] = 0; }); });
  Object.keys(paqs).forEach(id => { const q = paqs[id]; out[q.f] = (out[q.f] || 0) + Object.keys(q.m).reduce((a, p) => a + q.m[p], 0); });
  return out;
}
const MAX_FALLOS = 10, VENTANA = 900;          // 10 contraseñas mal en 15 minutos bloquean esa conexión
const MAX_CARGA = 100000000;                   // $1.000.000 por carga (en centavos)

async function redis(cmds) {
  const r = await fetch(URL_R + '/pipeline', { method: 'POST', headers: { Authorization: 'Bearer ' + TOK_R, 'Content-Type': 'application/json' }, body: JSON.stringify(cmds) });
  if (!r.ok) throw new Error('Base de datos: error ' + r.status);
  const j = await r.json();
  return j.map(x => { if (x && x.error) throw new Error('Base de datos: ' + x.error); return x ? x.result : null; });
}
function igual(a, b) { return crypto.timingSafeEqual(crypto.createHash('sha256').update(String(a)).digest(), crypto.createHash('sha256').update(String(b)).digest()); }
function rolDe(usuario, clave) {
  if (!clave) return '';
  const u = String(usuario || '').toLowerCase();
  if ((u === 'admin' || u === '') && CLAVES.admin && igual(clave, CLAVES.admin)) return 'admin';
  if ((u === 'playa' || u === '') && CLAVES.playa && igual(clave, CLAVES.playa)) return 'playa';
  return '';
}
function enviar(res, st, obj) { obj.ahora = ahoraAR(); res.statusCode = st; res.setHeader('Content-Type', 'application/json; charset=utf-8'); res.setHeader('Cache-Control', 'no-store'); res.end(JSON.stringify(obj)); }
async function leerCuerpo(req) {
  const b = req.body;
  if (b !== undefined && b !== null && b !== '') { if (typeof b === 'string') return JSON.parse(b); if (Buffer.isBuffer(b)) return JSON.parse(b.toString('utf8')); return b; }
  const partes = []; for await (const ch of req) partes.push(ch);
  const t = Buffer.concat(partes).toString('utf8'); return t ? JSON.parse(t) : {};
}
const ar = () => new Date(Date.now() - 3 * 3600 * 1000).toISOString();
const hoyAR = () => ar().slice(0, 10);
const ahoraAR = () => ar().slice(0, 19).replace('T', ' ');
/** Turno actual (hora Argentina): Mañana 06:30–14:30, Tarde 14:30–22:30, Noche 22:30–06:30 con la fecha del día que empieza a las 00:00. */
function turnoAR() {
  const a = ar(), min = Number(a.slice(11, 13)) * 60 + Number(a.slice(14, 16)), f0 = a.slice(0, 10);
  if (min >= 1350) return { turno: 'N', f: new Date(Date.parse(f0 + 'T12:00:00Z') + 86400000).toISOString().slice(0, 10) };
  return { turno: min < 390 ? 'N' : min < 870 ? 'M' : 'T', f: f0 };
}
const esFecha = f => /^\d{4}-\d{2}-\d{2}$/.test(f || '');
const esId = x => typeof x === 'string' && /^[\w|\-.:]{1,80}$/.test(x);
const hash = arr => { const o = {}; for (let i = 0; arr && i < arr.length; i += 2) o[arr[i]] = arr[i + 1]; return o; };
const num = o => { const r = {}; Object.keys(o).forEach(k => { r[k] = Number(o[k]); }); return r; };

module.exports = async function handler(req, res) {
  try {
    const q = new URL(req.url, 'http://local').searchParams;
    const lista = !!(URL_R && TOK_R && CLAVES.admin);
    if (q.has('ping')) return enviar(res, 200, { nube: lista, playa: !!CLAVES.playa });
    if (!lista) return enviar(res, 503, { error: 'La nube no está configurada: falta conectar la base Upstash Redis o cargar CLAVE_ADMIN en Vercel.' });

    const ip = String(req.headers['x-forwarded-for'] || req.headers['x-real-ip'] || '').split(',')[0].trim() || 'sin-ip';
    let clave = ''; try { clave = req.headers['x-clave'] ? decodeURIComponent(String(req.headers['x-clave'])) : ''; } catch (e) { clave = ''; }
    const usuario = String(req.headers['x-usuario'] || '').toLowerCase() === 'playa' ? 'playa' : 'admin';
    // sin contraseña no se toca la base (así nadie de afuera gasta el cupo gratis)
    if (!clave) return enviar(res, 401, { error: 'Usuario o contraseña incorrectos.' });
    const kf = 'propinas:fallos:' + usuario + ':' + ip; // el bloqueo es por usuario: un error en ADMIN no deja afuera a PLAYA
    const [fallos] = await redis([['GET', kf]]);
    if (Number(fallos) >= MAX_FALLOS) return enviar(res, 429, { error: 'Demasiados intentos fallidos. Probá de nuevo en 15 minutos.' });
    const rol = rolDe(usuario, clave);
    if (!rol) { await redis([['INCR', kf], ['EXPIRE', kf, VENTANA]]); return enviar(res, 401, { error: 'Usuario o contraseña incorrectos.' }); }
    const op = q.get('op') || '';

    /* ---------- lectura */
    if (req.method === 'GET') {
      if (rol === 'playa') {
        // PLAYA solo recibe los empleados habilitados y los paquetes del día del turno actual y del anterior (nada de propinas ni acumulados)
        const tf = turnoAR();
        const r = await redis([['GET', K.emp], ['LRANGE', K.pq, -150, -1], ['GET', K.pqdiaOk], ['HGETALL', K.pqdia]]);
        let acum = {};
        if (r[2] === '1') { const h = r[3] || []; for (let i = 0; i < h.length; i += 2) acum[h[i]] = Number(h[i + 1]) || 0; }
        else { // primera vez (o después de importar): se arma el acumulado por fecha con todos los paquetes
          acum = totalesPorDia(((await redis([['LRANGE', K.pq, 0, -1]]))[0] || []).map(x => JSON.parse(x)));
          const fs = Object.keys(acum);
          await redis([['DEL', K.pqdia]].concat(fs.length ? [['HSET', K.pqdia].concat([].concat.apply([], fs.map(f => [f, String(acum[f])])))] : []).concat([['SET', K.pqdiaOk, '1']]));
        }
        const desde = new Date(Date.parse(tf.f + 'T12:00:00Z') - 400 * 86400000).toISOString().slice(0, 10);
        Object.keys(acum).forEach(f => { if (f < desde || !acum[f]) delete acum[f]; });
        const lst = (r[1] || []).map(x => JSON.parse(x)), anul = new Set(lst.filter(x => x.t === 'obs' && x.accion === 'anular').map(x => x.ref));
        const ayer = new Date(Date.parse(tf.f + 'T12:00:00Z') - 86400000).toISOString().slice(0, 10); // también el día anterior: para el reporte del turno que acaba de terminar
        const paquetes = lst.filter(x => x.t === 'paq' && (x.f === tf.f || x.f === ayer) && !anul.has(x.id)); // los anulados no se muestran en la playa
        let emp = r[0] ? JSON.parse(r[0]) : null;
        if (!emp) { const [d] = await redis([['GET', K.datos]]); const x = d ? JSON.parse(d) : null; emp = { empresa: (x && x.config && x.config.empresa && x.config.empresa.nombre) || '', empleados: ((x && x.empleados) || []).filter(m => m.activo && m.s === 'PLAYA').map(m => ({ id: m.id, nombre: m.nombre, legajo: m.legajo || '' })) }; }
        return enviar(res, 200, { rol, hoy: tf.f, turno: tf.turno, empresa: emp.empresa, empleados: emp.empleados, tot: {}, dia: {}, cargas: [], paquetes, acum });
      }
      const desde = Math.max(0, parseInt(q.get('desde') || '0', 10) || 0);
      if (q.has('meta')) { const r = await redis([['MGET', K.rev, K.cambio], ['LLEN', K.ev], ['LLEN', K.pq]]); return enviar(res, 200, { rol, rev: Number(r[0][0] || 0), cambio: r[0][1] || '', n: r[1], npq: r[2] }); }
      const desdePq = Math.max(0, parseInt(q.get('desdePq') || '0', 10) || 0);
      const conDoc = q.get('doc') !== '0';
      const cmds = [['MGET', K.rev, K.cambio], ['LLEN', K.ev], ['LRANGE', K.ev, desde, -1], ['EXISTS', K.emp], ['LLEN', K.pq], ['LRANGE', K.pq, desdePq, -1]];
      if (conDoc) cmds.push(['GET', K.datos]);
      const r = await redis(cmds);
      const out = { rol, rev: Number(r[0][0] || 0), cambio: r[0][1] || '', n: r[1], desde, eventos: (r[2] || []).map(x => JSON.parse(x)), emp: !!r[3], npq: r[4], desdePq, paquetes: (r[5] || []).map(x => JSON.parse(x)), hoy: hoyAR() };
      if (conDoc) out.datos = r[6] ? JSON.parse(r[6]) : null;
      return enviar(res, 200, out);
    }

    if (req.method !== 'PUT' && req.method !== 'POST') { res.setHeader('Allow', 'GET, PUT, POST'); return enviar(res, 405, { error: 'Método no permitido.' }); }
    const b = await leerCuerpo(req);

    /* ---------- nueva propina (solo ADMIN: PLAYA únicamente arma paquetes) */
    if (op === 'carga' && rol === 'playa') return enviar(res, 403, { error: 'El usuario PLAYA solo arma paquetes. Las propinas las carga el administrador.' });
    if (op === 'carga') {
      const c = Number(b.c);
      if (!esId(b.id) || !esId(b.p)) return enviar(res, 400, { error: 'Datos inválidos.' });
      if (!(c > 0) || Math.round(c) !== c || c > MAX_CARGA) return enviar(res, 400, { error: 'Importe inválido.' });
      const f = rol === 'playa' ? hoyAR() : (esFecha(b.f) ? b.f : hoyAR());
      if (rol === 'playa') {
        const [e] = await redis([['GET', K.emp]]);
        const lista = e ? JSON.parse(e).empleados : [];
        if (!lista.some(x => x.id === b.p)) return enviar(res, 400, { error: 'El empleado no está habilitado.' });
      }
      const [nuevo] = await redis([['HSETNX', K.ids, b.id, '1']]);
      if (!nuevo) { const [n] = await redis([['LLEN', K.ev]]); return enviar(res, 200, { dup: true, n }); }
      const ev = { id: b.id, t: 'alta', f, p: b.p, c, ts: ahoraAR(), por: rol === 'playa' ? 'PLAYA' : 'ADMIN', quien: rol === 'playa' ? 'PLAYA' : String(b.quien || 'Administrador').slice(0, 60) };
      const r = await redis([['RPUSH', K.ev, JSON.stringify(ev)], ['HSET', K.c, ev.id, JSON.stringify({ p: ev.p, f, c })], ['HINCRBY', K.tot, ev.p, c], ['HINCRBY', dia(f), ev.p, c], ['RPUSH', evd(f), JSON.stringify(ev)]]);
      return enviar(res, 200, { ev, n: r[0] });
    }

    /* ---------- paquete de efectivo armado (ADMIN o PLAYA). No es un pago. */
    if (op === 'paquete') {
      if (rol === 'playa') { const tf = turnoAR(); b.turno = tf.turno; b.f = tf.f; } // PLAYA no elige: fecha y turno salen de la hora de Argentina
      if (!esId(b.id) || !['M', 'T', 'N'].includes(b.turno)) return enviar(res, 400, { error: 'Elegí el turno.' });
      const hoy = hoyAR();
      const manana = new Date(Date.parse(hoy + 'T12:00:00Z') + 86400000).toISOString().slice(0, 10);
      if (!esFecha(b.f) || b.f > manana) return enviar(res, 400, { error: 'Fecha inválida.' }); // la noche (desde 22:30) ya va con la fecha de mañana
      if (rol === 'playa' && b.f < new Date(Date.parse(hoy + 'T12:00:00Z') - 3 * 86400000).toISOString().slice(0, 10)) return enviar(res, 400, { error: 'Solo se pueden armar paquetes de los últimos 3 días.' });
      const items = Array.isArray(b.items) ? b.items : [];
      if (!items.length) return enviar(res, 400, { error: 'Agregá al menos un empleado.' });
      const [e] = await redis([['GET', K.emp]]);
      const lista = e ? JSON.parse(e).empleados : [], vistos = {};
      const limpios = [];
      for (const it of items) {
        if (!it || typeof it !== 'object') return enviar(res, 400, { error: 'Datos inválidos.' });
        const c = Number(it.c);
        if (!esId(it.p) || vistos[it.p]) return enviar(res, 400, { error: 'Empleado repetido o inválido.' });
        vistos[it.p] = 1;
        if (!(c > 0) || Math.round(c) !== c || c > MAX_CARGA) return enviar(res, 400, { error: 'Importe inválido.' });
        const m = lista.find(x => x.id === it.p);
        if (rol === 'playa' && !m) return enviar(res, 400, { error: 'Hay un empleado que no está habilitado.' });
        limpios.push({ p: it.p, nombre: m ? m.nombre : String(it.nombre || '').slice(0, 80), c });
      }
      const [nuevo] = await redis([['HSETNX', K.ids, b.id, '1']]);
      if (!nuevo) { // reintento del mismo paquete (por ejemplo, se cortó internet): se devuelve el ya guardado
        const ya = (((await redis([['LRANGE', K.pq, -300, -1]]))[0]) || []).map(x => JSON.parse(x)).find(x => x.t === 'paq' && x.id === b.id);
        if (ya) return enviar(res, 200, { paq: ya, dup: true });
      }
      const [n] = await redis([['INCR', K.pqseq]]);
      const paq = { id: b.id, t: 'paq', n, f: b.f, turno: b.turno, ts: ahoraAR(), items: limpios, total: limpios.reduce((a, x) => a + x.c, 0), por: rol === 'playa' ? 'PLAYA' : 'ADMIN', quien: rol === 'playa' ? 'Usuario PLAYA' : String(b.quien || 'Administrador').slice(0, 60) };
      const r = await redis([['RPUSH', K.pq, JSON.stringify(paq)], ['HINCRBY', K.pqdia, paq.f, paq.total]]);
      return enviar(res, 200, { paq, npq: r[0] });
    }

    if (rol !== 'admin') return enviar(res, 403, { error: 'El usuario PLAYA solo puede cargar propinas nuevas. Para corregir, pedíselo al administrador.' });

    /* ---------- corrección (solo ADMIN): el original no se borra, se agrega el cambio */
    if (op === 'corr') {
      const c = Number(b.c), motivo = String(b.motivo || '').trim();
      if (!esId(b.id) || !esId(b.ref)) return enviar(res, 400, { error: 'Datos inválidos.' });
      if (!(c >= 0) || Math.round(c) !== c || c > MAX_CARGA) return enviar(res, 400, { error: 'Importe inválido.' });
      if (motivo.length < 5) return enviar(res, 400, { error: 'Indicá el motivo de la corrección.' });
      const [act] = await redis([['HGET', K.c, b.ref]]);
      if (!act) return enviar(res, 404, { error: 'La carga no existe.' });
      const cur = JSON.parse(act), p = esId(b.p) ? b.p : cur.p;
      if (cur.c === c && cur.p === p) return enviar(res, 400, { error: 'No hay cambios para guardar.' });
      const [nuevo] = await redis([['HSETNX', K.ids, b.id, '1']]);
      if (!nuevo) { const [n] = await redis([['LLEN', K.ev]]); return enviar(res, 200, { dup: true, n }); }
      const ev = { id: b.id, t: 'corr', ref: b.ref, f: cur.f, p, pAntes: cur.p, c, antes: cur.c, ts: ahoraAR(), por: 'ADMIN', quien: String(b.quien || 'Administrador').slice(0, 60), motivo: motivo.slice(0, 300) };
      const r = await redis([['RPUSH', K.ev, JSON.stringify(ev)], ['HSET', K.c, b.ref, JSON.stringify({ p, f: cur.f, c })],
        ['HINCRBY', K.tot, cur.p, -cur.c], ['HINCRBY', K.tot, p, c], ['HINCRBY', dia(cur.f), cur.p, -cur.c], ['HINCRBY', dia(cur.f), p, c]]);
      return enviar(res, 200, { ev, n: r[0] });
    }

    /* ---------- revisión del administrador sobre un paquete (el comprobante original no cambia)
     * accion: 'ok' (está bien) · 'corr' (corrige importes: mueve la caja) · 'obs' (solo una nota) */
    if (op === 'paqobs') {
      const accion = ['ok', 'corr', 'obs', 'anular'].includes(b.accion) ? b.accion : (b.p ? 'corr' : 'obs');
      const texto = String(b.texto || '').trim() || (accion === 'ok' ? 'Revisado: está OK' : '');
      if (!esId(b.id) || !esId(b.ref)) return enviar(res, 400, { error: 'Datos inválidos.' });
      if (accion !== 'ok' && texto.length < 5) return enviar(res, 400, { error: accion === 'anular' ? 'Indicá el motivo de la anulación (al menos 5 letras).' : accion === 'corr' ? 'Indicá el motivo de la corrección (al menos 5 letras).' : 'Escribí la observación (al menos 5 letras).' });
      const todos = ((await redis([['LRANGE', K.pq, 0, -1]]))[0] || []).map(x => JSON.parse(x));
      const paq = todos.find(x => x.t === 'paq' && x.id === b.ref);
      if (!paq) return enviar(res, 404, { error: 'El paquete no existe.' });
      const act = Object.create(null); paq.items.forEach(i => { act[i.p] = i.c; });
      const obsPaq = todos.filter(x => x.t === 'obs' && x.ref === paq.id);
      if (obsPaq.some(o => o.accion === 'anular') && accion !== 'obs') return enviar(res, 400, { error: 'El paquete ya está anulado.' });
      obsPaq.forEach(o => { if (o.p) act[o.p] = o.c; (o.cambios || []).forEach(c => { act[c.p] = c.c; }); });
      const obs = { id: b.id, t: 'obs', ref: b.ref, n: paq.n, accion, ts: ahoraAR(), quien: String(b.quien || 'Administrador').slice(0, 60), texto: texto.slice(0, 500) };
      if (accion === 'corr') {
        const pedidos = Array.isArray(b.cambios) ? b.cambios : (b.p ? [{ p: b.p, c: b.c }] : []);
        const cambios = [];
        for (const x of pedidos) {
          if (!x || !esId(x.p)) return enviar(res, 400, { error: 'Corrección inválida.' });
          const c = Number(x.c);
          if (!Object.prototype.hasOwnProperty.call(act, x.p) || !(c >= 0) || Math.round(c) !== c || c > MAX_CARGA) return enviar(res, 400, { error: 'Corrección inválida.' });
          if (c !== act[x.p]) cambios.push({ p: x.p, antes: act[x.p], c });
        }
        if (!cambios.length) return enviar(res, 400, { error: 'No hay cambios de importe.' });
        obs.cambios = cambios;
      }
      const [nuevo] = await redis([['HSETNX', K.ids, b.id, '1']]);
      if (!nuevo) return enviar(res, 200, { dup: true });
      const mueve = accion === 'anular' ? -Object.keys(act).reduce((a, p) => a + act[p], 0) : (obs.cambios || []).reduce((a, x) => a + x.c - x.antes, 0);
      const r = await redis([['RPUSH', K.pq, JSON.stringify(obs)]].concat(mueve ? [['HINCRBY', K.pqdia, paq.f, mueve]] : []));
      return enviar(res, 200, { obs, npq: r[0] });
    }

    /* ---------- pasar a la nube las propinas que ya había (solo con la nube vacía) */
    if (op === 'importar') {
      const evs = Array.isArray(b.eventos) ? b.eventos.filter(e => e && esId(e.id)) : [];
      const [n0] = await redis([['LLEN', K.ev]]);
      if (n0 > 0) return enviar(res, 409, { error: 'La nube ya tiene propinas cargadas.', n: n0 });
      const pqs = Array.isArray(b.paquetes) ? b.paquetes.filter(e => e && esId(e.id)) : [];
      if (pqs.length) {
        const [np] = await redis([['LLEN', K.pq]]);
        if (!np) { const maxN = pqs.reduce((a, x) => Math.max(a, Number(x.n) || 0), 0); await redis([['SET', K.pqdiaOk, '0'], ['RPUSH', K.pq].concat(pqs.map(x => JSON.stringify(x))), ['HSET', K.ids].concat([].concat.apply([], pqs.map(x => [x.id, '1']))), ['SET', K.pqseq, String(maxN)]]); }
      }
      if (!evs.length) return enviar(res, 200, { n: 0 });
      const C = {};
      evs.forEach(e => {
        if (e.t === 'alta' && esFecha(e.f)) C[e.id] = { p: e.p, f: e.f, c: Math.max(0, Math.round(Number(e.c) || 0)) };
        else if (e.t === 'corr' && C[e.ref]) { C[e.ref].c = Math.max(0, Math.round(Number(e.c) || 0)); if (e.p) C[e.ref].p = e.p; }
      });
      const tot = {}, dias = {};
      Object.keys(C).forEach(id => { const x = C[id]; tot[x.p] = (tot[x.p] || 0) + x.c; (dias[x.f] = dias[x.f] || {})[x.p] = ((dias[x.f] || {})[x.p] || 0) + x.c; });
      const cmds = [], lote = 500;
      for (let i = 0; i < evs.length; i += lote) {
        const parte = evs.slice(i, i + lote);
        cmds.push(['RPUSH', K.ev].concat(parte.map(e => JSON.stringify(e))));
        cmds.push(['HSET', K.ids].concat([].concat.apply([], parte.map(e => [e.id, '1']))));
      }
      const ids = Object.keys(C);
      for (let i = 0; i < ids.length; i += lote) cmds.push(['HSET', K.c].concat([].concat.apply([], ids.slice(i, i + lote).map(id => [id, JSON.stringify(C[id])]))));
      if (Object.keys(tot).length) cmds.push(['HSET', K.tot].concat([].concat.apply([], Object.keys(tot).map(p => [p, String(tot[p])]))));
      Object.keys(dias).forEach(f => {
        cmds.push(['HSET', dia(f)].concat([].concat.apply([], Object.keys(dias[f]).map(p => [p, String(dias[f][p])]))));
        const altas = evs.filter(e => e.t === 'alta' && e.f === f);
        if (altas.length) cmds.push(['RPUSH', evd(f)].concat(altas.map(e => JSON.stringify(e))));
      });
      await redis(cmds);
      const [n] = await redis([['LLEN', K.ev]]);
      return enviar(res, 200, { n });
    }

    /* ---------- datos generales: empleados, caja, entregas, ajustes (solo ADMIN) */
    if (op === '' || op === 'datos') {
      const r = await redis([['GET', K.rev], ['GET', K.respHoy]]);
      if (!b || !b.datos || b.datos.app !== 'propinas-bcia') return enviar(res, 400, { error: 'Datos inválidos.' });
      const rev = Number(r[0] || 0);
      if (Number(b.base) !== rev) { const m = await redis([['GET', K.cambio]]); return enviar(res, 409, { error: 'Hay datos más nuevos en la nube.', rev, cambio: m[0] || '' }); }
      const datos = b.datos; delete datos.eventos; delete datos.paquetes; datos.cargas = {};
      const txt = JSON.stringify(datos), nuevo = rev + 1, cambio = String(b.cambio || ''), hoy = hoyAR();
      const emp = { empresa: (datos.config && datos.config.empresa && datos.config.empresa.nombre) || '', empleados: (datos.empleados || []).filter(m => m.activo && m.s === 'PLAYA').map(m => ({ id: m.id, nombre: m.nombre, legajo: m.legajo || '' })) };
      const cmds = [['SET', K.datos, txt], ['SET', K.rev, String(nuevo)], ['SET', K.cambio, cambio], ['SET', K.emp, JSON.stringify(emp)]];
      if (r[1] !== hoy) cmds.push(['SET', 'propinas:respaldo:' + hoy, txt, 'EX', 2592000], ['SET', K.respHoy, hoy]);
      await redis(cmds);
      return enviar(res, 200, { rol, rev: nuevo, cambio });
    }
    return enviar(res, 400, { error: 'Operación desconocida.' });
  } catch (e) {
    return enviar(res, 500, { error: e.message || String(e) });
  }
};
