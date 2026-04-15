import express, { Request, Response, Router } from 'express';
import fs from 'fs';
import path from 'path';
import ExcelJS from 'exceljs';
import { createFreshClient } from '../config/supabase';

const router: Router = express.Router();

type DistributionExportRequest = {
  fragebogen_ids?: string[];
  question_ids?: string[];
  chains?: string[];
};

type BasicQuestion = {
  id: string;
  question_text: string;
  type: string;
};

type BasicFragebogen = {
  id: string;
  name: string;
};

type BasicResponse = {
  id: string;
  fragebogen_id: string;
  market_id: string | null;
  gebietsleiter_id: string | null;
  status: string;
  completed_at: string | null;
};

type BasicAnswer = {
  response_id: string;
  question_id: string;
  answer_boolean: boolean | null;
  answered_at: string | null;
};

type BasicMarket = {
  id: string;
  name: string | null;
  chain: string | null;
};

type BasicUser = {
  id: string;
  first_name: string | null;
  last_name: string | null;
};

const TEMPLATE_CANDIDATES = [
  path.resolve(process.cwd(), 'src/templates/fragebogen_distribution_template.xlsx'),
  path.resolve(process.cwd(), 'templates/fragebogen_distribution_template.xlsx')
];

const readTable = async <T>(tableCandidates: string[], selectQuery: string, whereIn?: { column: string; values: string[] }): Promise<T[]> => {
  const client = createFreshClient();
  let lastError: any = null;

  for (const table of tableCandidates) {
    let query: any = client.from(table).select(selectQuery);
    if (whereIn && whereIn.values.length > 0) {
      query = query.in(whereIn.column, whereIn.values);
    }

    const { data, error } = await query;
    if (!error) return (data || []) as T[];
    lastError = error;
  }

  throw lastError || new Error(`All table candidates failed: ${tableCandidates.join(', ')}`);
};

const chunk = <T,>(items: T[], size: number): T[][] => {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
};

const getMonthKey = (isoDate: string): string => {
  const date = new Date(isoDate);
  if (Number.isNaN(date.getTime())) return 'Unbekannt';
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
};

const getDisplayMonth = (monthKey: string): string => {
  const [year, month] = monthKey.split('-');
  if (!year || !month) return monthKey;
  return `${month}.${year}`;
};

const createWorkbook = async (): Promise<ExcelJS.Workbook> => {
  const workbook = new ExcelJS.Workbook();
  const templatePath = TEMPLATE_CANDIDATES.find(candidate => fs.existsSync(candidate));
  if (templatePath) {
    await workbook.xlsx.readFile(templatePath);
  }
  return workbook;
};

const ensureSheet = (workbook: ExcelJS.Workbook, name: string): ExcelJS.Worksheet => {
  return workbook.getWorksheet(name) || workbook.addWorksheet(name);
};

router.post('/fragebogen/distribution-export.xlsx', async (req: Request, res: Response) => {
  try {
    const body = req.body as DistributionExportRequest;
    const fragebogenIds = Array.from(new Set((body.fragebogen_ids || []).filter(Boolean)));
    const questionIds = Array.from(new Set((body.question_ids || []).filter(Boolean)));
    const selectedChains = Array.from(new Set((body.chains || []).map(c => c.trim()).filter(Boolean)));

    if (fragebogenIds.length === 0) {
      return res.status(400).json({ error: 'Mindestens ein Fragebogen muss ausgewählt sein.' });
    }
    if (questionIds.length === 0) {
      return res.status(400).json({ error: 'Mindestens ein Ja/Nein-Item muss ausgewählt sein.' });
    }

    const [fragebogenRows, questionRows] = await Promise.all([
      readTable<BasicFragebogen>(['fb_fragebogen', 'fragebogen'], 'id,name', { column: 'id', values: fragebogenIds }),
      readTable<BasicQuestion>(['fb_questions', 'questions'], 'id,question_text,type', { column: 'id', values: questionIds })
    ]);

    if (fragebogenRows.length === 0) {
      return res.status(400).json({ error: 'Keine gültigen Fragebögen gefunden.' });
    }

    const invalidQuestion = questionRows.find(q => q.type !== 'yesno');
    if (invalidQuestion) {
      return res.status(400).json({ error: `Nur Ja/Nein-Fragen erlaubt. Ungültige Frage: ${invalidQuestion.question_text}` });
    }
    if (questionRows.length !== questionIds.length) {
      return res.status(400).json({ error: 'Mindestens eine ausgewählte Frage wurde nicht gefunden.' });
    }

    const responses = await readTable<BasicResponse>(
      ['fb_responses', 'responses'],
      'id,fragebogen_id,market_id,gebietsleiter_id,status,completed_at',
      { column: 'fragebogen_id', values: fragebogenIds }
    );

    const completedResponses = responses.filter(r =>
      r.status === 'completed'
      && !!r.market_id
      && !!r.completed_at
    );

    const responseIds = completedResponses.map(r => r.id);
    const marketIds = Array.from(new Set(completedResponses.map(r => r.market_id).filter(Boolean) as string[]));
    const glIds = Array.from(new Set(completedResponses.map(r => r.gebietsleiter_id).filter(Boolean) as string[]));

    const [marketRows, userRows] = await Promise.all([
      marketIds.length > 0
        ? readTable<BasicMarket>(['markets'], 'id,name,chain', { column: 'id', values: marketIds })
        : Promise.resolve([]),
      glIds.length > 0
        ? readTable<BasicUser>(['users'], 'id,first_name,last_name', { column: 'id', values: glIds })
        : Promise.resolve([])
    ]);

    const marketById = new Map(marketRows.map(m => [m.id, m]));
    const userById = new Map(userRows.map(u => [u.id, u]));
    const fragebogenById = new Map(fragebogenRows.map(f => [f.id, f]));
    const questionById = new Map(questionRows.map(q => [q.id, q]));

    const filteredResponses = completedResponses.filter(response => {
      if (selectedChains.length === 0) return true;
      if (!response.market_id) return false;
      const market = marketById.get(response.market_id);
      const chain = (market?.chain || '').trim();
      return selectedChains.includes(chain);
    });

    const filteredResponseIds = filteredResponses.map(r => r.id);
    const answers: BasicAnswer[] = [];
    for (const group of chunk(filteredResponseIds, 500)) {
      if (group.length === 0) continue;
      const answerRows = await readTable<BasicAnswer>(
        ['fb_response_answers', 'response_answers'],
        'response_id,question_id,answer_boolean,answered_at',
        { column: 'response_id', values: group }
      );
      answers.push(...answerRows.filter(a => questionIds.includes(a.question_id) && a.answer_boolean !== null));
    }

    const responseById = new Map(filteredResponses.map(r => [r.id, r]));

    const rows: Array<{
      monthKey: string;
      monthLabel: string;
      fragebogenId: string;
      fragebogenName: string;
      questionId: string;
      questionLabel: string;
      responseId: string;
      answerBoolean: boolean;
      answerLabel: 'Ja' | 'Nein';
      marketId: string;
      marketName: string;
      chain: string;
      glId: string;
      glName: string;
    }> = [];

    answers.forEach(answer => {
      const response = responseById.get(answer.response_id);
      if (!response || !response.completed_at || !response.market_id) return;

      const question = questionById.get(answer.question_id);
      if (!question) return;

      const market = marketById.get(response.market_id);
      const chain = (market?.chain || '').trim();
      if (selectedChains.length > 0 && !selectedChains.includes(chain)) return;

      const monthKey = getMonthKey(response.completed_at);
      const user = response.gebietsleiter_id ? userById.get(response.gebietsleiter_id) : null;
      const glName = `${user?.first_name || ''} ${user?.last_name || ''}`.trim() || 'Unbekannt';

      rows.push({
        monthKey,
        monthLabel: getDisplayMonth(monthKey),
        fragebogenId: response.fragebogen_id,
        fragebogenName: fragebogenById.get(response.fragebogen_id)?.name || response.fragebogen_id,
        questionId: answer.question_id,
        questionLabel: question.question_text || answer.question_id,
        responseId: answer.response_id,
        answerBoolean: Boolean(answer.answer_boolean),
        answerLabel: answer.answer_boolean ? 'Ja' : 'Nein',
        marketId: response.market_id,
        marketName: market?.name || response.market_id,
        chain,
        glId: response.gebietsleiter_id || '',
        glName
      });
    });

    const workbook = await createWorkbook();
    const rawSheet = ensureSheet(workbook, 'RawData');
    const itemSheet = ensureSheet(workbook, 'ItemDistribution_Monthly');
    const customerSheet = ensureSheet(workbook, 'CustomerDistribution_Monthly');
    const adSheet = ensureSheet(workbook, 'ADDistribution_Monthly');
    const chartDataSheet = ensureSheet(workbook, 'ChartData');
    const chartSheet = ensureSheet(workbook, 'Chart');

    rawSheet.columns = [
      { header: 'Monat', key: 'monthLabel', width: 12 },
      { header: 'Fragebogen', key: 'fragebogenName', width: 28 },
      { header: 'Item', key: 'questionLabel', width: 42 },
      { header: 'Antwort', key: 'answerLabel', width: 10 },
      { header: 'Store', key: 'marketName', width: 28 },
      { header: 'Handelskette', key: 'chain', width: 20 },
      { header: 'AD-Mitarbeiter', key: 'glName', width: 26 },
      { header: 'Response ID', key: 'responseId', width: 38 },
      { header: 'Question ID', key: 'questionId', width: 38 }
    ];

    rows.forEach(row => rawSheet.addRow(row));

    const months = Array.from(new Set(rows.map(r => r.monthKey))).sort();
    const selectedQuestionRows = questionRows
      .filter(q => questionIds.includes(q.id))
      .map(q => ({ id: q.id, label: q.question_text || q.id }));

    itemSheet.addRow(['Monat', ...selectedQuestionRows.map(q => q.label), 'Alle ausgewählten Items']);

    months.forEach(month => {
      const monthRows = rows.filter(r => r.monthKey === month);
      const values: (string | number)[] = [getDisplayMonth(month)];

      selectedQuestionRows.forEach(question => {
        const itemRows = monthRows.filter(r => r.questionId === question.id);
        const total = itemRows.length;
        const yes = itemRows.filter(r => r.answerBoolean).length;
        const distribution = total > 0 ? yes / total : 0;
        values.push(distribution);
      });

      const monthTotal = monthRows.length;
      const monthYes = monthRows.filter(r => r.answerBoolean).length;
      values.push(monthTotal > 0 ? monthYes / monthTotal : 0);

      itemSheet.addRow(values);
    });

    const itemColumnCount = selectedQuestionRows.length + 2;
    for (let col = 2; col <= itemColumnCount; col += 1) {
      itemSheet.getColumn(col).numFmt = '0.00%';
      itemSheet.getColumn(col).width = col === itemColumnCount ? 28 : 24;
    }
    itemSheet.getColumn(1).width = 12;

    customerSheet.addRow(['Monat', 'Handelskette', 'Kunde', 'Ja', 'Gesamt', 'Distribution']);
    const customerMap = new Map<string, { yes: number; total: number }>();
    rows.forEach(row => {
      const key = `${row.monthKey}__${row.chain}__${row.marketName}`;
      const current = customerMap.get(key) || { yes: 0, total: 0 };
      current.total += 1;
      if (row.answerBoolean) current.yes += 1;
      customerMap.set(key, current);
    });

    Array.from(customerMap.entries())
      .sort((a, b) => a[0].localeCompare(b[0], 'de'))
      .forEach(([key, value]) => {
        const [monthKey, chain, marketName] = key.split('__');
        customerSheet.addRow([
          getDisplayMonth(monthKey),
          chain,
          marketName,
          value.yes,
          value.total,
          value.total > 0 ? value.yes / value.total : 0
        ]);
      });
    customerSheet.getColumn(6).numFmt = '0.00%';

    adSheet.addRow(['Monat', 'AD-Mitarbeiter', 'Ja', 'Gesamt', 'Distribution']);
    const adMap = new Map<string, { yes: number; total: number }>();
    rows.forEach(row => {
      const key = `${row.monthKey}__${row.glName}`;
      const current = adMap.get(key) || { yes: 0, total: 0 };
      current.total += 1;
      if (row.answerBoolean) current.yes += 1;
      adMap.set(key, current);
    });

    Array.from(adMap.entries())
      .sort((a, b) => a[0].localeCompare(b[0], 'de'))
      .forEach(([key, value]) => {
        const [monthKey, glName] = key.split('__');
        adSheet.addRow([
          getDisplayMonth(monthKey),
          glName,
          value.yes,
          value.total,
          value.total > 0 ? value.yes / value.total : 0
        ]);
      });
    adSheet.getColumn(5).numFmt = '0.00%';

    chartDataSheet.addRow(['Monat', ...selectedQuestionRows.map(q => q.label), 'Alle ausgewählten Items']);
    for (let i = 2; i <= itemSheet.rowCount; i += 1) {
      const row = itemSheet.getRow(i);
      chartDataSheet.addRow(row.values as any[]);
    }
    for (let col = 2; col <= itemColumnCount; col += 1) {
      chartDataSheet.getColumn(col).numFmt = '0.00%';
    }

    chartSheet.getCell('A1').value = 'Distribution Export (Monatlich, Ja/Nein)';
    chartSheet.getCell('A2').value = `Fragebögen: ${fragebogenRows.map(f => f.name).join(', ')}`;
    chartSheet.getCell('A3').value = selectedChains.length > 0
      ? `Handelsketten: ${selectedChains.join(', ')}`
      : 'Handelsketten: Alle';
    chartSheet.getCell('A5').value = 'Hinweis: Für die automatische Liniengrafik bitte die Chart-Template-Datei hinterlegen.';
    chartSheet.getCell('A6').value = 'Template-Name: fragebogen_distribution_template.xlsx';
    chartSheet.getCell('A7').value = 'Datenquelle für Linienchart: Sheet "ChartData".';

    if (rows.length === 0) {
      rawSheet.addRow({
        monthLabel: 'Keine Daten',
        fragebogenName: 'Für die aktuelle Auswahl wurden keine besuchten Märkte mit Ja/Nein-Antworten gefunden.'
      });
    }

    const buffer = await workbook.xlsx.writeBuffer();
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="fragebogen_distribution_${new Date().toISOString().slice(0, 10)}.xlsx"`);
    res.send(Buffer.from(buffer));
  } catch (error: any) {
    console.error('Fragebogen distribution export failed:', error);
    res.status(500).json({
      error: error?.message || 'Distribution-Export fehlgeschlagen'
    });
  }
});

export default router;

