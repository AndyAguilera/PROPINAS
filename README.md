# Propinas · Beraza y Cia SRL (Playa)

`index.html` (la app) + `api/datos.js` (guardado en la nube en Vercel).

## Usuarios
- **ADMINISTRADOR**: acceso total (cargas, correcciones, pagos, caja, reportes, control de paquetes).
- **PLAYA**: usuario compartido del personal. Carga las propinas del día de cada empleado y arma paquetes de efectivo. No puede ver días anteriores, corregir ni borrar.

## Configuración en Vercel
1. **Storage › Create Database › Upstash for Redis**, conectada al proyecto (crea `KV_REST_API_URL` y `KV_REST_API_TOKEN`).
2. **Settings › Environment Variables**:
   - `CLAVE_ADMIN`: contraseña del administrador.
   - `CLAVE_PLAYA`: contraseña del usuario PLAYA.
3. **Deployments › Redeploy** después de cambiar variables.

Las contraseñas nunca van en el código. Las propinas, correcciones y paquetes se guardan como movimientos que no se borran.

## Impresión de paquetes
Comprobante térmico para Epson Receipt de 80 mm o 58 mm (se elige en la pantalla "Armar entrega" o en Ajustes).
En el cuadro de impresión: impresora Epson, márgenes "Ninguno", escala 100 %.

Usar con Google Chrome o Microsoft Edge.
