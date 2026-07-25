# FraudFlux analyst dashboard

React and TypeScript operations dashboard for the FraudFlux FastAPI service.

## Run locally

Start the backend first, then:

```powershell
cd dashboard
npm install
npm run dev
```

Open `http://localhost:5173`.

The dashboard reads `VITE_API_URL` from the repository-level `.env`. If it is
hosted on another origin, also update the comma-separated
`FRAUDFLUX_CORS_ORIGINS` value in that file.

## Validation

```powershell
npm run typecheck
npm run build
```

The dashboard listens to `/events/stream` with Server-Sent Events. A dashboard
event refreshes the aggregate metrics, transaction stream, and alert queue.
If the stream disconnects, the UI clearly shows its reconnecting state.
