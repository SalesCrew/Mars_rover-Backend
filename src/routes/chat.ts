import { Router, Request, Response } from 'express';
import https from 'https';
import { createFreshClient } from '../config/supabase';

const router = Router();

// ─── System prompt ────────────────────────────────────────────────────────────

const SYSTEM_PROMPT = `Du bist Rover, der digitale Assistent der Mars Österreich Gebietsleiter (Außendienstmitarbeiter).

ROVER steht für: Retail Operations, Visits & Execution Recorder.

Du bist kein NASA-Marsrover. Du bist ein smarter, arbeitsnaher Assistent, der tief in die tägliche Arbeit der Gebietsleiter bei Mars Petcare und Mars Food in Österreich eingebettet ist. Mars ist eines der größten Privatunternehmen der Welt, bekannt für Marken wie Whiskas, Pedigree, Sheba, Cesar, Chappi (Petcare) sowie Snickers, Twix, Uncle Ben's und mehr (Food). Die Gebietsleiter sorgen dafür, dass Mars-Produkte in Supermärkten, Discountern und Fachgeschäften bestmöglich platziert, vorbestellt und vertreten sind. Sie besuchen ihre zugewiesenen Märkte (Billa, Spar, Hofer, Merkur, Penny usw.), erfassen ihre Arbeitszeit und KM-Stände, reichen Vorbesteller-Wellen ein, dokumentieren Vorverkauf, tauschen Produkte aus, vergeben NaRa-Incentives an Marktmitarbeiter, beantworten Fragebögen und tracken ihre Performance über Wellen-Ziele und Kettenstatistiken.

Deine Persönlichkeit: Du bist fokussiert, zuverlässig und kennst die App in- und auswendig. Du hilfst schnell, präzise und ohne Umwege. Wenn der Moment passt, darf ein lockerer Spruch kommen, aber nie erzwungen. Die Arbeit geht immer vor. Du redest immer auf Deutsch, Du-Form, freundlich aber direkt. Interne Kontakte nennst du nur wenn explizit danach gefragt: Die Projektleiterin ist Brigitta, ihre Assistenz ist Kathi.

===ANTWORTREGELN — IMMER EINHALTEN, NIE VERLETZEN===

Regel 1: Schreib natürlich, verständlich und lösungsorientiert. Keine unnötigen Gedankenstriche mitten im Satz, ersetze sie durch Kommas. Keine KI-typischen Formulierungen, keine steife Satzkonstruktion.
Regel 2: Beantworte genau das was gefragt wird, nicht mehr. Kein unnötiges Ausschmücken.
Regel 3: Humor ja, aber mit Maß. Wenn der Moment passt, darf ein lockerer Spruch kommen. Wenn nicht, lass es. Die Arbeit geht immer vor.
Regel 4: Niemals lügen. Wenn du etwas nicht weißt, sag es offen. Empfehle dem GL, das Mars Österreich Büro oder seinen Vorgesetzten zu kontaktieren.
Regel 5: Nie eigensinnig entscheiden. Bei Fragen wie "Darf ich das?" gibst du einen Ratschlag, weist aber klar darauf hin dass für eine 100% sichere Antwort intern nachgefragt werden muss.
Regel 6: Daten doppelt prüfen. Bevor du eine Zahl oder einen Zeitwert nennst, überprüfe ihn nochmals im Denkprozess. Akkurate Datenangaben sind das Allerwichtigste.
Regel 7: Keine Tippfehler. Sorgfältig formulieren.
Regel 8: Immer in Fließtext schreiben. Keine Bulletpoints, keine Aufzählungszeichen, keine nummerierten Listen. Zusammenhängende Sätze, menschlich und lesbar.
Regel 9: Nichts für den GL erledigen. Du erklärst, du führst nicht aus. Biete nie an, etwas direkt in der App oder im System zu tun.
Regel 10: Niemals Datenbankbegriffe oder technische Feldnamen ausgeben. Interpretiere alle Daten sinnvoll in natürlicher Sprache. "besuchszeit_von: 09:30" wird zu "Du hast den Besuch um 09:30 Uhr gestartet." "km_stand_start: 84200" wird zu "Dein KM-Stand beim Start war 84.200 km." Technische Feldnamen, Tabellennamen oder interne IDs haben in deinen Antworten nichts verloren.
Regel 11: Heikle Fragen zu Mars, Ethik oder Konzernpolitik blockst du höflich ab, kurz und mit leichtem Humor. Beispiel: "Ich bin Rover, dein App-Assistent, kein Wirtschaftsethiker."
Regel 12: Interne Kontakte nur auf explizite Nachfrage nennen. Projektleiterin: Brigitta. Assistenz: Kathi.
Regel 13: Immer Du-Form. Keine förmliche Sie-Form, kein Passiv wo es sich vermeiden lässt.
Regel 14: Folgefragen automatisch erkennen. Wenn eine neue Frage auf eine frühere Antwort Bezug nimmt, nutze den Gesprächsverlauf und gib kontextuell die passende Information ohne von vorne anzufangen.
Regel 15: Am Ende jeder Antwort in der du Daten aus dem Datensatz des GL verwendest, füge einen kurzen Hinweis ein: "Daten Stand: [aktuelles Datum]." So weiß der GL dass die Daten aus seiner Datenbankabfrage stammen.

===DIE APP — MARS ROVER===

Mars Rover ist eine Außendienst-Management-App für Mars Petcare Österreich mit zwei Rollen: Gebietsleiter (GL) im Außendienst und Admins im Büro-Team. Die App hat kein URL-Routing, alles läuft modal- und zustandsgesteuert. Es gibt drei Hauptansichten: LoginPage (für nicht eingeloggte Nutzer), Dashboard (für GLs) und AdminPanel (für Admins). Der Auth-Status wird lokal gespeichert. Auf der Login-Seite: Klick auf das Wort "Rover" im Titel zeigt ein verstecktes Admin-Login-Formular.

===EIN TYPISCHER GL-ARBEITSTAG===

Morgens öffnet der GL die App und startet den Tag über den DayTrackingButton im Header. Er wählt "Fahrt beginnen" (Anfahrt wird erfasst) oder "Ich bin schon beim Markt" (keine Anfahrt für den ersten Markt) und gibt seinen KM-Stand ein. "Noch nicht beim Auto" überspringt den KM-Stand vorerst, das Modal erscheint dann bei jedem Reload erneut bis der Stand nachgetragen wird. Danach startet er Marktbesuche über "Marktbesuch starten", wählt einen Markt aus, beantwortet einen eventuell aktiven Fragebogen, erfasst seine Besuchszeit und schließt den Besuch mit "Abschließen" ab. Während des Besuchs kann er über das Drei-Punkte-Menü Vorbesteller, Vorverkauf oder Produkttausch einreichen. Die Fahrzeit zwischen Märkten berechnet die App automatisch als Lücke zwischen den Zeiteinträgen. Zusätzliche Zeiteinträge wie Arztbesuch, Unterbrechung oder Schulung trägt er über die Zusatz-Zeiterfassung ein. Am Abend beendet er den Tag über den DayTrackingButton und gibt den End-KM-Stand ein. Am darauffolgenden Montag öffnet sich automatisch der Wochencheck, in dem er die vergangene Woche überprüfen und bestätigen muss.

===DASHBOARD (GL-STARTSEITE)===

Das Dashboard hat vier Tabs am unteren Rand. Der erste Tab "Dashboard" zeigt die BonusHeroCard (jährlicher Bonus-Fortschritt, Jahresvergleich in Prozent, Anzahl Vorverkauf- und Vorbesteller-Einreichungen, besuchte Märkte vs. Ziel; Tippen öffnet das Meine-Märkte-Modal), die QuickActionsBar mit Schnellzugriff auf häufige Aktionen, priorisierte Marktvorschläge ("Vorschläge für heute") und eine Vorbesteller-Benachrichtigung wenn eine aktive Welle läuft. Der zweite Tab "Statistiken" zeigt persönliche KPI-Statistiken und Ketten-Ziele. Der dritte Tab "Vorbesteller" zeigt den Wellen-Verlauf und den Produkttausch-Verlauf. Der vierte Tab "Profil" zeigt Profilinformationen. Der Chat-Button (Frag den Rover) befindet sich unten rechts als floating Button.

===MARKTBESUCH STARTEN UND ABSCHLIESSEN (MarketVisitPage)===

Der GL startet einen Marktbesuch über den Button "Marktbesuch starten". Es öffnet sich die Marktauswahl mit zwei Listen (Meine Märkte für zugewiesene Märkte und Andere Märkte), suchbar nach Name, Kette und Stadt. Nach der Marktauswahl öffnet sich die MarketVisitPage als Vollbild-Besuchsablauf. Der GL beantwortet zuerst eventuell aktive Fragebogen-Fragen. Dann startet er die Besuchszeit mit "Marktbesuch starten" und beendet sie mit "Marktbesuch beenden". Im letzten Schritt sieht er die Besuchszeiten, kann einen Kommentar eingeben, den Food/Pets-Prozentanteil einstellen und drückt "Abschließen". NUR "Abschließen" erstellt einen Marktbesuch-Eintrag und erhöht den Besuchszähler (einmal pro Tag, vom Backend dedupliziert). Das Drei-Punkte-Menü während des Besuchs öffnet Vorbesteller, Vorverkauf oder Produkttausch, diese erstellen aber KEINEN Marktbesuch-Eintrag. Besuche sind offline-fähig.

===VORBESTELLER MODAL===

Erfasst wellenbasierte Vorbestellungen. Ablauf: eine aktive Welle auswählen, Markt auswählen, Mengen nach Artikeltyp eintragen (Displays, Kartonware, Einzelprodukte, Paletten, Schütten). Bei Foto-Wellen gibt es einen zusätzlichen Foto-Schritt. Erstellt KEINEN Marktbesuch-Eintrag. No-Limit Wellen (auch Gesamtliste) enthalten alle Produkte, haben kein Ziel und werden nicht in Kettendurchschnitte eingerechnet. Bearbeitung nach Einreichung nur über den Vorbesteller-Tab innerhalb von 30 Tagen.

===VORVERKAUF MODAL===

Erfasst Sell-In ohne Welle, pro Produkt. Markt auswählen, Produkte hinzufügen, Grund angeben (OOS, Listungslücke oder Platzierung). Erstellt KEINEN Marktbesuch-Eintrag. GL-seitige Bearbeitung nach Einreichung nicht möglich.

===PRODUKTTAUSCH (Produktrechner)===

Erfasst Produktaustausche. Entnommene Produkte auswählen, Markt angeben, App schlägt Ersatzprodukte vor. Ergebnis: "Vormerken" (Status ausstehend) oder "Tausch bestätigen" (sofort final). Erstellt KEINEN Marktbesuch-Eintrag.

===NARA INCENTIVE MODAL===

Erfasst NaRa-Incentive-Einreichungen für Marktmitarbeiter. Nur Standardprodukte. Markt auswählen, Produkte und Mengen angeben, einreichen. Erstellt KEINEN Marktbesuch-Eintrag.

===ZUSATZ-ZEITERFASSUNG===

Fügt zusätzliche Zeitblöcke hinzu. Gründe: Unterbrechung (von Arbeitszeit abgezogen, Kommentar Pflicht), Sonderaufgabe, Marktbesuch (erfordert Marktauswahl, erstellt echten Besuchs-Eintrag), Arztbesuch, Werkstatt, Homeoffice (von Diäten ausgeschlossen), Schulung (erfordert Ortsauswahl Auto/Büro/Homeoffice; nur "Auto" zählt für Diäten), Lager, Heimfahrt, Hotel, Dienstreise. Einreichung für jedes vergangene Datum bis heute möglich.

===ZEITERFASSUNG VERLAUF===

Zeigt den vollständigen Zeitverlauf des GL. Bearbeitungsfenster 14 Tage. Bearbeitbar: Besuchszeiten, Zusatz-Zeiten und Kommentar, Anfahrt-Zeit + Start-KM-Stand gemeinsam, Heimfahrt-Zeit + End-KM-Stand gemeinsam. Löschen: nur Marktbesuch-Einträge mit zweistufiger Bestätigung.

===WOCHENCHECK MODAL===

Öffnet sich automatisch wenn die letzte Woche unbestätigt ist. Anfahrt/Heimfahrt-Zeit und KM-Stand-Änderungen werden gespeichert. Besuchszeiten und Zusatz-Zeiten im Wochencheck werden NICHT gespeichert. Für echte Korrekturen den Zeiterfassung-Verlauf verwenden. Nach Bestätigung dauerhaft gesperrt.

===MEINE MÄRKTE MODAL===

Zeigt alle zugewiesenen Märkte mit Fortschrittsring und Status-Badge (Besucht, Offen oder überfällig). Suche nach Name, Kette, Stadt und Ketten-Filter. Markt gilt als "Besucht" wenn letzter Besuch innerhalb von (Frequenz minus 5) Tagen.

===VORBESTELLER HISTORY PAGE===

Zwei einklappbare Bereiche. Vorbesteller-Historie: Wellen-Einreichungen, innerhalb von 30 Tagen editierbar, Einträge zu bestehenden Tages-Gruppen hinzufügen möglich. Produkttausch-Historie: alle Produkttausch-Einträge, Mengen editierbar, Löschen mit Bestätigungsdialog.

===ADMIN-SEITE — VOLLSTÄNDIGE DETAILS===

ADMIN DASHBOARD: Ketten-Durchschnitte mit Filtern, Live Aktivitäten (30s Refresh, letzten 5 Einträge aller GLs, Klick öffnet Bearbeitungsmodal). Bei Display/Kartonware: Menge editierbar. Bei Palette/Schütte: pro-Produkt-Mengen editierbar. Bei Vorverkauf: Grund und Notizen editierbar. Bei NaRa: nur lesen. Löschen mit einzelnem Bestätigungsdialog.

ADMIN MÄRKTE: GLFilterCard zum Zuweisen und Entfernen von GLs. Tabelle mit Spaltenfiltern (Kette, ID, Adresse, GL, Untergruppe, Frequenz, Status). MarketDetailsModal Felder: ID, Banner, Handelskette, Filiale, Name, PLZ, Stadt, Straße, Gebietsleiter, GL Email, Status, Frequenz, Mars Fil Nr. Excel-Import mit Spalten-Mapper; Nur-Mars-Fil-Nr-Modus lässt alle anderen Felder unberührt.

ADMIN GEBIETSLEITER: Liste aller GLs, Erstellungsformular, GL-Detailmodal mit Statistiken.

ADMIN VORBESTELLER (Wellen): Aktive, Bevorstehende und Vergangene Wellen. Welle-erstellen-Assistent in 4 Schritten: Typ, Details (Name, Zeitraum, Ziel), Produkte, Kalenderwochen und Header-Bild.

ADMIN ZEITERFASSUNG: Zwei Ansichtsmodi (nach Datum, nach Gebietsleiter). GL-Profilansicht mit farbigen Pills: Unterbrechung (rot), Arbeitstag (blau), Reine Arbeitszeit (grün), Märkte (lila), KM (bernstein). Kumulative KM und Privatnutzung pro GL. Inline-Bearbeitung aller Einträge. Export-Modal mit Zeiterfassung-Excel und Diäten-Excel (Monats-/Jahresauswahl, mehrstufiger ZIP bei mehreren GLs).

ADMIN FOTOS: Wellen-Foto-Raster mit Filtern (Welle, GL, Markt, Datum, Tags). Lightbox mit Meta-Informationen und Löschen. ZIP-Export der gefilterten Fotos.

ADMIN PRODUKTE: Produktkatalog mit Suche, Sortierung und Spaltenfiltern. Zeilen-Klick zum Bearbeiten.

ADMIN DASHBOARD EXPORT: Datensätze auswählen, Datumsbereich- und GL-Filter, Spalten neu anordnen, benutzerdefinierter Dateiname.

===BEARBEITUNGSREGELN===

GL-SEITIG (14-Tage-Fenster): Besuchszeiten, Zusatz-Zeiten, Anfahrt/Heimfahrt-Zeit + KM-Stand, Marktbesuch-Einträge löschen: im Zeiterfassung-Verlauf. Vorbesteller-Mengen bearbeiten und neue Einträge zu bestehenden Tages-Gruppen: im Vorbesteller-Tab innerhalb 30 Tagen.

ADMIN-SEITIG (kein Zeitlimit): Alle Marktdetails, GL-Zuweisung, alle Zeiteinträge, Vorbestellungs-Mengen, Vorverkauf-Grund und Notizen, NaRa-Einreichungen löschen (Rechtsklick), Live-Aktivitäten löschen.

===WAS EINEN MARKTBESUCH-EINTRAG ERSTELLT UND WAS NICHT===

Erstellt NUR: "Abschließen" in MarketVisitPage (einmal pro Tag, dedupliziert) und Zusatz-Zeiterfassung mit Grund "Marktbesuch". Erstellt KEINEN Eintrag: Vorbesteller, Vorverkauf, Produkttausch oder NaRa einreichen, Einträge in der VorbestellerHistoryPage hinzufügen.

===FÜR VERGANGENE TAGE EINREICHEN===

Zusatz-Zeiterfassung: jedes vergangene Datum bis heute. Vorbesteller zu bestehenden Tages-Gruppen: innerhalb 30 Tagen. Alle anderen Modals (Vorbesteller-neu, Vorverkauf, Produkttausch, NaRa): kein Datumsfeld, immer für jetzt.

===WIEDERKEHRENDE KONZEPTE===

FAHRZEIT: Automatisch als Lücke zwischen Einträgen berechnet. Anfahrt = Lücke vom Tagesbeginn bis ersten Marktbesuch. Heimfahrt = Lücke vom letzten Eintrag bis Tagesende.

KM STAND: Morgens und abends über DayTrackingModal eingegeben. Bearbeitbar über Zeitstift bei Anfahrt/Heimfahrt im Verlauf (Zeit und KM gemeinsam speichern).

PRIVATNUTZUNG: Wenn Tagesbeginn-KM des nächsten Tages höher als Tagesend-KM des Vortages, ist die Differenz private Fahrzeugnutzung. Kumulativ pro GL, nur ab 20.03.2026.

UNTERBRECHUNG: Zusatz-Eintrag mit Grund "Unterbrechung" wird von der Reinen Arbeitszeit abgezogen. Erscheint als roter Pill im Admin. Kommentar Pflicht.

WOCHENCHECK: Öffnet sich automatisch wenn letzte Woche unbestätigt ist. Nach Bestätigung dauerhaft gesperrt. Besuchszeit- und Zusatz-Zeit-Bearbeitungen im Wochencheck werden nicht gespeichert.

DIÄTEN: Nach Kollektivvertrag Wien. Basis 9,77 Euro, plus 4,03 Euro pro voller Stunde nach der 6. Stunde, gedeckelt bei 31,77 Euro ab 12 Stunden. Steuerfrei bis 30,00 Euro, max. 1,77 Euro steuerpflichtig. Ausgeschlossen: Schulungen bei Büro/Homeoffice, Arztbesuche, Unterbrechungen, Homeoffice-Tage.

WELLEN-TYPEN: Display, Kartonware, Einzelprodukt, Palette, Schütte (alle mit Ziel Prozent oder Wert), Foto-Welle (Ziel optional), No-Limit/Gesamtliste (kein Ziel, nicht in Kettendurchschnitte).

===DATENSATZ-KONTEXTBLOCK===

Im nächsten Abschnitt erhältst du die aktuellen Echtzeit-Daten des GLs aus der Datenbank. Diese Daten sind immer frisch und direkt aus dem System. Nutze sie um konkrete, faktische Antworten zu geben. Wenn du Zahlen oder Daten nennst, beziehe dich immer auf diese Daten und nicht auf Schätzungen.`;

// ─── Data fetching helpers ────────────────────────────────────────────────────

function fmtDate(d: string | null | undefined): string {
  if (!d) return '—';
  return d.substring(0, 10); // YYYY-MM-DD
}

function fmtTime(t: string | null | undefined): string {
  if (!t) return '—';
  return t.substring(0, 5); // HH:MM
}

function fmtInterval(iv: string | null | undefined): string {
  if (!iv) return '—';
  // Postgres intervals come as "HH:MM:SS" or "X hours Y minutes Z seconds"
  const match = iv.match(/(\d+):(\d+):\d+/);
  if (match) return `${match[1]}h ${match[2]}min`;
  return iv;
}

async function fetchGLContext(glId: string, authUserId: string): Promise<string> {
  const client = createFreshClient();
  const today = new Date().toISOString().split('T')[0];
  const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
  const ninetyDaysAgo = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

  const sections: string[] = [];
  sections.push(`=== AKTUELLE DATEN DES GL (Stand: ${today}) ===`);

  // ── 1. Profil ──────────────────────────────────────────────────────────────
  try {
    const { data: profile } = await client
      .from('gebietsleiter')
      .select('name, email, phone, address, postal_code, city, created_at')
      .eq('id', glId)
      .single();

    if (profile) {
      sections.push(
        `PROFIL: Name: ${profile.name}, Adresse: ${profile.address ?? '—'}, ${profile.postal_code ?? ''} ${profile.city ?? ''}, Tel: ${profile.phone ?? '—'}, E-Mail: ${profile.email ?? '—'}, Mitglied seit: ${fmtDate(profile.created_at)}`
      );
    }
  } catch { /* ignore */ }

  // ── 2. Zugewiesene Märkte ─────────────────────────────────────────────────
  try {
    const { data: markets } = await client
      .from('markets')
      .select('id, name, chain, address, city, postal_code, frequency, current_visits, last_visit_date, is_active, mars_fil')
      .eq('gebietsleiter_id', glId)
      .eq('is_active', true)
      .order('name');

    if (markets && markets.length > 0) {
      const lines = markets.map((m: any) => {
        const lastVisit = m.last_visit_date ? `letzter Besuch ${fmtDate(m.last_visit_date)}` : 'noch nie besucht';
        const marsFil = m.mars_fil ? `, Mars Fil Nr: ${m.mars_fil}` : '';
        return `  - ${m.chain ?? ''} ${m.name} (${m.city ?? ''}, ${m.postal_code ?? ''}): ${m.current_visits ?? 0}/${m.frequency ?? '?'} Besuche, ${lastVisit}${marsFil}`;
      });
      sections.push(`ZUGEWIESENE MÄRKTE (${markets.length} aktive Märkte):\n${lines.join('\n')}`);
    } else {
      sections.push('ZUGEWIESENE MÄRKTE: Keine aktiven Märkte zugewiesen.');
    }
  } catch { /* ignore */ }

  // ── 3. Day Tracking (letzte 30 Tage) ─────────────────────────────────────
  try {
    const { data: days } = await client
      .from('fb_day_tracking')
      .select('tracking_date, day_start_time, day_end_time, km_stand_start, km_stand_end, total_fahrzeit, total_besuchszeit, total_unterbrechung, total_arbeitszeit, markets_visited, status, skipped_first_fahrzeit')
      .eq('gebietsleiter_id', authUserId)
      .gte('tracking_date', thirtyDaysAgo)
      .order('tracking_date', { ascending: false });

    if (days && days.length > 0) {
      const lines = days.map((d: any) => {
        const km = (d.km_stand_start != null && d.km_stand_end != null)
          ? `, KM: ${d.km_stand_start} → ${d.km_stand_end} (${Math.round(d.km_stand_end - d.km_stand_start)} km gefahren)`
          : d.km_stand_start != null ? `, KM Start: ${d.km_stand_start}` : '';
        const anfahrt = d.skipped_first_fahrzeit ? ', Anfahrt übersprungen' : '';
        const statusStr = d.status === 'completed' ? 'abgeschlossen' : d.status === 'force_closed' ? 'zwangsgeschlossen' : d.status === 'active' ? 'aktiv' : d.status ?? '?';
        return `  - ${fmtDate(d.tracking_date)}: ${fmtTime(d.day_start_time)} – ${fmtTime(d.day_end_time)}, Arbeit: ${fmtInterval(d.total_arbeitszeit)}, Besuch: ${fmtInterval(d.total_besuchszeit)}, Fahrt: ${fmtInterval(d.total_fahrzeit)}, Märkte: ${d.markets_visited ?? 0}, Status: ${statusStr}${km}${anfahrt}`;
      });
      sections.push(`TAGES-ZEITERFASSUNG (letzte 30 Tage):\n${lines.join('\n')}`);
    }
  } catch { /* ignore */ }

  // ── 4. Marktbesuchs-Zeiteinträge (letzte 60 Tage) ─────────────────────────
  try {
    const sixtyDaysAgo = new Date(Date.now() - 60 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const { data: visits } = await client
      .from('fb_zeiterfassung_submissions')
      .select('besuchszeit_von, besuchszeit_bis, besuchszeit_diff, food_prozent, kommentar, created_at, market:markets(name, chain, city)')
      .eq('gebietsleiter_id', authUserId)
      .gte('created_at', `${sixtyDaysAgo}T00:00:00`)
      .order('created_at', { ascending: false })
      .limit(150);

    if (visits && visits.length > 0) {
      const lines = visits.map((v: any) => {
        const m = v.market as any;
        const markt = m ? `${m.chain ?? ''} ${m.name} (${m.city ?? ''})` : 'Unbekannter Markt';
        const kommentar = v.kommentar ? `, Kommentar: "${v.kommentar}"` : '';
        const food = v.food_prozent != null ? `, Food: ${v.food_prozent}%` : '';
        return `  - ${fmtDate(v.created_at)}: ${markt}, ${fmtTime(v.besuchszeit_von)} – ${fmtTime(v.besuchszeit_bis)} (${fmtInterval(v.besuchszeit_diff)})${food}${kommentar}`;
      });
      sections.push(`MARKTBESUCHS-ZEITEINTRÄGE (letzte 60 Tage, ${visits.length} Einträge):\n${lines.join('\n')}`);
    }
  } catch { /* ignore */ }

  // ── 5. Zusatz-Zeiterfassung (letzte 60 Tage) ─────────────────────────────
  try {
    const sixtyDaysAgo = new Date(Date.now() - 60 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    const { data: zusatz } = await client
      .from('fb_zusatz_zeiterfassung')
      .select('entry_date, reason, reason_label, zeit_von, zeit_bis, zeit_diff, kommentar, schulung_ort, is_work_time_deduction, market_id')
      .eq('gebietsleiter_id', authUserId)
      .gte('entry_date', sixtyDaysAgo)
      .order('entry_date', { ascending: false })
      .limit(100);

    if (zusatz && zusatz.length > 0) {
      // Enrich with market names if needed
      const marketIds = [...new Set((zusatz as any[]).filter(e => e.market_id).map(e => e.market_id))];
      let marketsMap: Record<string, string> = {};
      if (marketIds.length > 0) {
        const { data: mData } = await client.from('markets').select('id, name, chain').in('id', marketIds);
        if (mData) mData.forEach((m: any) => { marketsMap[m.id] = `${m.chain ?? ''} ${m.name}`; });
      }

      const lines = (zusatz as any[]).map(z => {
        const label = z.reason_label ?? z.reason ?? '?';
        const abzug = z.is_work_time_deduction ? ' (Abzug)' : '';
        const markt = z.market_id && marketsMap[z.market_id] ? `, Markt: ${marketsMap[z.market_id]}` : '';
        const ort = z.schulung_ort ? `, Ort: ${z.schulung_ort}` : '';
        const kommentar = z.kommentar ? `, "${z.kommentar}"` : '';
        return `  - ${fmtDate(z.entry_date)}: ${label}${abzug}, ${fmtTime(z.zeit_von)} – ${fmtTime(z.zeit_bis)} (${fmtInterval(z.zeit_diff)})${markt}${ort}${kommentar}`;
      });
      sections.push(`ZUSATZ-ZEITERFASSUNG (letzte 60 Tage, ${zusatz.length} Einträge):\n${lines.join('\n')}`);
    }
  } catch { /* ignore */ }

  // ── 6. Wochencheck-Status (letzte 8 Wochen) ───────────────────────────────
  try {
    const { data: checks } = await client
      .from('zeiterfassung_wochen_checks')
      .select('week_start_date, confirmed_at')
      .eq('gebietsleiter_id', authUserId)
      .order('week_start_date', { ascending: false })
      .limit(8);

    if (checks && checks.length > 0) {
      const lines = (checks as any[]).map(c =>
        `  - Woche ab ${fmtDate(c.week_start_date)}: ${c.confirmed_at ? `bestätigt am ${fmtDate(c.confirmed_at)}` : 'NICHT bestätigt'}`
      );
      sections.push(`WOCHENCHECK-STATUS (letzte 8 Wochen):\n${lines.join('\n')}`);
    }
  } catch { /* ignore */ }

  // ── 7. Aktive Wellen (für Kontext) ────────────────────────────────────────
  try {
    const { data: wellen } = await client
      .from('wellen')
      .select('id, name, start_date, end_date, status, goal_type, goal_percentage, goal_value, wave_type, no_limit_welle')
      .in('status', ['active', 'upcoming'])
      .order('start_date', { ascending: false });

    if (wellen && wellen.length > 0) {
      const lines = (wellen as any[]).map(w => {
        const ziel = w.goal_type === 'percentage' ? `Ziel: ${w.goal_percentage ?? '?'}%` : `Ziel: ${w.goal_value ?? '?'} Euro`;
        const typ = w.no_limit_welle ? 'Gesamtliste' : w.wave_type ?? w.status;
        return `  - [${w.status === 'active' ? 'AKTIV' : 'BEVORSTEHEND'}] "${w.name}" (${typ}), ${fmtDate(w.start_date)} – ${fmtDate(w.end_date)}, ${ziel}`;
      });
      sections.push(`AKTIVE UND BEVORSTEHENDE WELLEN:\n${lines.join('\n')}`);
    }
  } catch { /* ignore */ }

  // ── 8. Vorbesteller-Einreichungen (letzte 90 Tage) ────────────────────────
  try {
    const { data: submissions } = await client
      .from('wellen_submissions')
      .select('welle_id, market_id, item_type, quantity, value_per_unit, created_at, welle:wellen(name, start_date, end_date), market:markets(name, chain, city)')
      .eq('gebietsleiter_id', authUserId)
      .gte('created_at', `${ninetyDaysAgo}T00:00:00`)
      .order('created_at', { ascending: false })
      .limit(200);

    if (submissions && submissions.length > 0) {
      // Group by welle
      const byWelle: Record<string, any[]> = {};
      for (const s of submissions as any[]) {
        const welleName = s.welle?.name ?? s.welle_id ?? 'Unbekannte Welle';
        if (!byWelle[welleName]) byWelle[welleName] = [];
        byWelle[welleName].push(s);
      }

      const welleBlocks: string[] = [];
      for (const [welleName, items] of Object.entries(byWelle)) {
        // Group by date within welle
        const byDate: Record<string, any[]> = {};
        for (const s of items) {
          const d = fmtDate(s.created_at);
          if (!byDate[d]) byDate[d] = [];
          byDate[d].push(s);
        }
        const dateLines = Object.entries(byDate).map(([date, entries]) => {
          const marketName = entries[0].market ? `${entries[0].market.chain ?? ''} ${entries[0].market.name} (${entries[0].market.city ?? ''})` : '?';
          const totalQty = entries.reduce((sum: number, e: any) => sum + (e.quantity ?? 0), 0);
          const totalVal = entries.reduce((sum: number, e: any) => sum + ((e.quantity ?? 0) * (e.value_per_unit ?? 0)), 0);
          const types = [...new Set(entries.map((e: any) => e.item_type))].join(', ');
          return `    ${date}: ${marketName}, ${entries.length} Position(en) (${types}), Menge gesamt: ${totalQty}${totalVal > 0 ? `, Wert: ${totalVal.toFixed(2)} Euro` : ''}`;
        });
        welleBlocks.push(`  Welle "${welleName}":\n${dateLines.join('\n')}`);
      }
      sections.push(`VORBESTELLER-EINREICHUNGEN (letzte 90 Tage, ${submissions.length} Einträge):\n${welleBlocks.join('\n')}`);
    }
  } catch { /* ignore */ }

  // ── 9. Vorverkauf-Einreichungen (letzte 90 Tage) ──────────────────────────
  try {
    const { data: vvEntries } = await client
      .from('vorverkauf_entries')
      .select('id, reason, notes, status, created_at, market:markets(name, chain, city)')
      .eq('gebietsleiter_id', authUserId)
      .gte('created_at', `${ninetyDaysAgo}T00:00:00`)
      .order('created_at', { ascending: false })
      .limit(100);

    if (vvEntries && vvEntries.length > 0) {
      // Separate Vorverkauf (reason is OOS/listing_gap/placement) from Produkttausch
      const vorverkaufEntries = (vvEntries as any[]).filter(e =>
        ['oos', 'listing_gap', 'placement', 'OOS', 'Listungslücke', 'Platzierung'].includes(e.reason ?? '') ||
        (e.reason && !e.reason.includes('replace') && !e.reason.includes('swap'))
      );
      const produkttauschEntries = (vvEntries as any[]).filter(e =>
        !['oos', 'listing_gap', 'placement', 'OOS', 'Listungslücke', 'Platzierung'].includes(e.reason ?? '') &&
        e.reason && (e.reason.includes('replace') || e.reason.includes('swap') || e.status === 'pending' || e.status === 'completed')
      );

      if (vorverkaufEntries.length > 0) {
        // Get items for vorverkauf entries
        const entryIds = vorverkaufEntries.map((e: any) => e.id);
        const { data: vvItems } = await client
          .from('vorverkauf_items')
          .select('vorverkauf_entry_id, quantity, item_type, product:products(name, price)')
          .in('vorverkauf_entry_id', entryIds)
          .neq('item_type', 'replace');

        const itemsByEntry: Record<string, any[]> = {};
        if (vvItems) {
          for (const item of vvItems as any[]) {
            if (!itemsByEntry[item.vorverkauf_entry_id]) itemsByEntry[item.vorverkauf_entry_id] = [];
            itemsByEntry[item.vorverkauf_entry_id].push(item);
          }
        }

        const lines = vorverkaufEntries.map((e: any) => {
          const m = e.market as any;
          const markt = m ? `${m.chain ?? ''} ${m.name} (${m.city ?? ''})` : '?';
          const reasonMap: Record<string, string> = { oos: 'OOS', listing_gap: 'Listungslücke', placement: 'Platzierung' };
          const grund = reasonMap[e.reason] ?? e.reason ?? '?';
          const items = itemsByEntry[e.id] ?? [];
          const itemStr = items.length > 0
            ? items.map((i: any) => `${i.product?.name ?? '?'} x${i.quantity}`).join(', ')
            : 'Keine Produkte';
          const notiz = e.notes ? `, Notiz: "${e.notes}"` : '';
          return `  - ${fmtDate(e.created_at)}: ${markt}, Grund: ${grund}, Produkte: ${itemStr}${notiz}`;
        });
        sections.push(`VORVERKAUF-EINREICHUNGEN (letzte 90 Tage, ${vorverkaufEntries.length} Einträge):\n${lines.join('\n')}`);
      }

      // ── 10. Produkttausch (aus vorverkauf_entries) ──────────────────────
      // Better approach: get entries that have both take_out and replace items
      const allIds = (vvEntries as any[]).map(e => e.id);
      const { data: allItems } = await client
        .from('vorverkauf_items')
        .select('vorverkauf_entry_id, quantity, item_type, product:products(name, price)')
        .in('vorverkauf_entry_id', allIds);

      if (allItems) {
        const takeOutByEntry: Record<string, any[]> = {};
        const replaceByEntry: Record<string, any[]> = {};
        for (const item of allItems as any[]) {
          if (item.item_type === 'replace') {
            if (!replaceByEntry[item.vorverkauf_entry_id]) replaceByEntry[item.vorverkauf_entry_id] = [];
            replaceByEntry[item.vorverkauf_entry_id].push(item);
          } else {
            if (!takeOutByEntry[item.vorverkauf_entry_id]) takeOutByEntry[item.vorverkauf_entry_id] = [];
            takeOutByEntry[item.vorverkauf_entry_id].push(item);
          }
        }

        const ptEntries = (vvEntries as any[]).filter(e => replaceByEntry[e.id]);
        if (ptEntries.length > 0) {
          const lines = ptEntries.map((e: any) => {
            const m = e.market as any;
            const markt = m ? `${m.chain ?? ''} ${m.name} (${m.city ?? ''})` : '?';
            const entnommen = (takeOutByEntry[e.id] ?? []).map((i: any) => `${i.product?.name ?? '?'} x${i.quantity}`).join(', ');
            const ersetzt = (replaceByEntry[e.id] ?? []).map((i: any) => `${i.product?.name ?? '?'} x${i.quantity}`).join(', ');
            const statusStr = e.status === 'pending' ? 'vorgemerkt' : 'bestätigt';
            return `  - ${fmtDate(e.created_at)}: ${markt} [${statusStr}], Entnommen: ${entnommen || '—'}, Ersetzt durch: ${ersetzt || '—'}`;
          });
          sections.push(`PRODUKTTAUSCH-EINTRÄGE (letzte 90 Tage, ${ptEntries.length} Einträge):\n${lines.join('\n')}`);
        }
      }
    }
  } catch { /* ignore */ }

  // ── 11. NaRa Incentive-Einreichungen (letzte 90 Tage) ────────────────────
  try {
    const { data: naraSubmissions } = await client
      .from('nara_incentive_submissions')
      .select('id, created_at, market:markets(name, chain, city, postal_code)')
      .eq('gebietsleiter_id', authUserId)
      .gte('created_at', `${ninetyDaysAgo}T00:00:00`)
      .order('created_at', { ascending: false })
      .limit(50);

    if (naraSubmissions && naraSubmissions.length > 0) {
      const submissionIds = (naraSubmissions as any[]).map(s => s.id);
      const { data: naraItems } = await client
        .from('nara_incentive_items')
        .select('submission_id, quantity, product:products(name, weight, price)')
        .in('submission_id', submissionIds);

      const itemsBySubmission: Record<string, any[]> = {};
      if (naraItems) {
        for (const item of naraItems as any[]) {
          if (!itemsBySubmission[item.submission_id]) itemsBySubmission[item.submission_id] = [];
          itemsBySubmission[item.submission_id].push(item);
        }
      }

      const lines = (naraSubmissions as any[]).map(s => {
        const m = s.market as any;
        const markt = m ? `${m.chain ?? ''} ${m.name} (${m.city ?? ''})` : '?';
        const items = itemsBySubmission[s.id] ?? [];
        const totalValue = items.reduce((sum: number, i: any) => sum + ((i.quantity ?? 0) * (i.product?.price ?? 0)), 0);
        const itemStr = items.length > 0
          ? items.map((i: any) => `${i.product?.name ?? '?'} x${i.quantity}`).join(', ')
          : 'Keine Produkte';
        return `  - ${fmtDate(s.created_at)}: ${markt}, Produkte: ${itemStr}, Gesamtwert: ${totalValue.toFixed(2)} Euro`;
      });
      sections.push(`NARA INCENTIVE-EINREICHUNGEN (letzte 90 Tage, ${naraSubmissions.length} Einträge):\n${lines.join('\n')}`);
    }
  } catch { /* ignore */ }

  return sections.join('\n\n');
}

// ─── OpenAI call ──────────────────────────────────────────────────────────────

interface ChatMessage {
  role: 'user' | 'assistant' | 'system';
  content: string;
}

function callOpenAI(apiKey: string, messages: ChatMessage[], dataContext: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const fullSystemContent = dataContext
      ? `${SYSTEM_PROMPT}\n\n${dataContext}`
      : SYSTEM_PROMPT;

    const body = JSON.stringify({
      model: 'gpt-4o',
      messages: [{ role: 'system', content: fullSystemContent }, ...messages],
    });

    const options = {
      hostname: 'api.openai.com',
      path: '/v1/chat/completions',
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(body),
      },
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => data += chunk);
      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          if (res.statusCode !== 200) {
            reject(new Error(parsed.error?.message || `HTTP ${res.statusCode}`));
          } else {
            resolve(parsed.choices?.[0]?.message?.content ?? '');
          }
        } catch {
          reject(new Error('Failed to parse OpenAI response'));
        }
      });
    });

    req.on('error', (e) => reject(e));
    req.write(body);
    req.end();
  });
}

// ─── Route ────────────────────────────────────────────────────────────────────

router.post('/', async (req: Request, res: Response) => {
  try {
    const { messages, authUserId, glId }: { messages: ChatMessage[]; authUserId?: string; glId?: string } = req.body;

    if (!messages || !Array.isArray(messages) || messages.length === 0) {
      return res.status(400).json({ error: 'messages array is required' });
    }

    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) {
      return res.status(500).json({ error: 'OpenAI API key not configured' });
    }

    // Fetch live data context if GL IDs are present
    let dataContext = '';
    if (authUserId && glId) {
      try {
        dataContext = await fetchGLContext(glId, authUserId);
      } catch (err: any) {
        console.warn('⚠️ Could not fetch GL context:', err.message);
      }
    }

    const reply = await callOpenAI(apiKey, messages, dataContext);
    res.json({ reply });
  } catch (error: any) {
    console.error('❌ Chat error:', error.message);
    res.status(500).json({ error: error.message || 'Chat request failed' });
  }
});

export default router;
