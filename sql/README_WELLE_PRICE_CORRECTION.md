# Wellen: gespeicherte Buchungspreise korrigieren

## Ablauf

Nach jedem erfolgreichen Bearbeiten/Speichern einer Welle prueft das Frontend
`GET /api/wellen/:id/submission-prices`. Es vergleicht die aktuell gespeicherten
Produktpreise mit allen bestehenden Buchungen dieser Welle, nicht nur mit den
Aenderungen des gerade geschlossenen Formulars.

Nur nach ausdruecklicher Bestaetigung wird
`POST /api/wellen/:id/submission-prices` mit dem Vorschau-Token aufgerufen.
Beide Routen verlangen einen authentifizierten Admin. Preise, Produkt-IDs,
fremde Wellen-IDs und die Admin-ID werden nicht aus dem POST-Body uebernommen.

## Datenbank und Rollout

Vor Bereitstellung der Backend-Route die Migration
`supabase/migrations/20260914100721_welle_submission_price_correction.sql`
auf dem vorgesehenen Supabase-Projekt installieren. Sie legt ausschliesslich
eine RPC-Funktion und die Audit-Tabelle an; sie korrigiert keine Buchungen.
Danach Backend und Frontend bereitstellen. Fehlt die Migration, meldet die
Oberflaeche einen fehlgeschlagenen Preisvergleich, ohne eine Uebernahme zu behaupten.

Die RPC-Funktion ist SECURITY INVOKER mit festem search_path und nur fuer
service_role ausfuehrbar. Audit-Daten sind mit RLS und ohne Client-Grants geschuetzt.
Der Advisor-Hinweis [RLS Enabled No Policy](https://supabase.com/docs/guides/database/database-linter?lint=0008_rls_enabled_no_policy)
ist fuer diese ausschliesslich serverseitige Audit-Tabelle beabsichtigt: Clients
duerfen das Protokoll weder lesen noch schreiben.

Die Korrektur gleicht Wellen-ID, Produkttyp und Produkt-ID ab. Bei Paletten und
Schuetten muss auch die Elternzuordnung zu dieser Welle passen. Keine Zuordnung
ueber Namen oder globale Produktpreise. Fehlende/ungueltige Wellenpreise oder
nicht mehr zuordenbare Produkte werden ausgelassen und in der Vorschau gezaehlt.

Nur `wellen_submissions.value_per_unit` wird geaendert. Mengen, IDs,
Besuchszeitpunkte, Fotos, Markt- und GL-Zuordnungen bleiben unveraendert.
Alt-/Neupreise und Akteur werden in `wellen_submission_price_corrections`
innerhalb derselben Transaktion protokolliert. Kein Audit-Erfolg bedeutet
auch keine Preisuebernahme. Die bestehenden Ansichten/Exporte verwenden nach
erneutem Laden die korrigierten Buchungspreise; die Preisquellenlogik wurde
hier nicht generell umgestellt.

Ein SHA-256-Token bindet die Vorschau an Welle, Produktpreise und alle Buchungen.
Bei der Bestaetigung werden die betroffenen Wellen-/Produkt-/Buchungszeilen
gesperrt und das Token erneut verglichen. Bei Abweichung: HTTP 409, keine
Preisuebernahme. Ein verlorenes POST-Ergebnis wird nicht blind wiederholt:
die UI verlangt zuerst eine frische Vorschau und gegebenenfalls eine neue
Bestaetigung. Alte Buchungsduplikate bleiben separate Zeilen.

## Isolierte Tests

`npm run test:price-correction` fuehrt echte PostgreSQL-Funktionstests in einer
PGlite-In-Memory-Datenbank sowie HTTP-Tests mit einem injizierten RPC-Stub aus.
Keine Produktionsverbindung, keine echten Buchungen.

Die Frontend-Fixture liegt im Frontend-Repository unter
`tests/welle-price-preview.html`. Sie faengt jeden fetch ab und leitet nichts
an ein echtes Backend weiter. Mit laufendem lokalen Vite und einem verfuegbaren
Playwright-Paket: `node tests/welle-price-browser.mjs` im Frontend-Verzeichnis.
Das Paket kann auch ueber NODE_PATH aus dem lokalen Codex-Runtime-Bundle kommen.
Die Tests pruefen den unveraenderten Bearbeitungsdurchlauf vom Dashboard,
Zuordnungen, Bestaetigung/Ablehnung, Fehler, Doppelklick und mobile Darstellung.

Fehlercodes sind im Frontend-Dokument `ERROR_CODES.md` dokumentiert.
