# FraudFlux analyst dashboard

React and TypeScript operations dashboard for the FraudFlux FastAPI service.

## Run locally

Start the backend first, then:

```powershell
cd dashboard
npm install
Copy-Item .env.example .env
npm run dev
```

Open `http://localhost:5173`.

`VITE_API_URL` selects the FastAPI base URL. If the dashboard is hosted on
another origin, also add that origin to the backend's comma-separated
`FRAUDFLUX_CORS_ORIGINS` setting.

## Validation

```powershell
npm run typecheck
npm run build
```

The dashboard listens to `/events/stream` with Server-Sent Events. A dashboard
event refreshes the aggregate metrics, transaction stream, and alert queue.
If the stream disconnects, the UI clearly shows its reconnecting state.
