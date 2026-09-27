# Trace explorer

The Phase 2 React/TypeScript explorer consumes the documented `/api/v1` query API. It displays only trace-derived data. Search times are entered as UTC clock values and sent as UTC RFC3339; duration fields are decimal milliseconds converted exactly to integer nanoseconds for the API.

```sh
npm ci
npm test
npm run build
```

For development, `npm run dev` proxies `/api` to the local Go API at port 18081. Production assets are emitted to `dist/` for the Go server to serve. Vite supplies the small build/dev pipeline; Vitest and Testing Library exercise user-visible behavior in jsdom. No client router or state library is needed for the single explorer screen. System fonts render without network access.
