# Trace explorer

The React/TypeScript app consumes the documented `/api/v1` trace and incident query APIs. The trace explorer displays stored spans and their relationships. The incident view compares fixed baseline/current windows using the server's versioned rule, presents uncertainty, and opens supporting traces in their returned evidence intervals. Both views display only trace-derived data. Search and evaluation times are entered as UTC clock values and sent as UTC RFC3339; trace-search duration fields are decimal milliseconds converted exactly to integer nanoseconds for the API.

```sh
npm ci
npm test
npm run build
```

For development, `npm run dev` proxies `/api` to the local Go API at port 18081. Production assets are emitted to `dist/` for the Go server to serve. Vite supplies the small build/dev pipeline; Vitest and Testing Library exercise user-visible behavior in jsdom. No client router or state library is needed for the two local views. System fonts render without network access.
