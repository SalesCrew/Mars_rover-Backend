#!/usr/bin/env python3
import json
import sys
from collections import OrderedDict, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import xlsxwriter


def die(message: str, code: int = 1) -> None:
    sys.stderr.write(message + "\n")
    sys.exit(code)


def parse_input(path: Path) -> Dict[str, Any]:
    if not path.exists():
        die(f"Input file not found: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        die(f"Failed to parse input JSON: {exc}")


def unique_months(rows: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
    month_map: "OrderedDict[str, str]" = OrderedDict()
    for row in sorted(rows, key=lambda item: str(item.get("monthKey", ""))):
        month_key = str(row.get("monthKey", "")).strip()
        if not month_key:
            continue
        month_label = str(row.get("monthLabel", "")).strip() or month_key
        if month_key not in month_map:
            month_map[month_key] = month_label
    return list(month_map.items())


def build_workbook(payload: Dict[str, Any], output_path: Path) -> None:
    rows: List[Dict[str, Any]] = payload.get("rows", []) or []
    selected_questions: List[Dict[str, str]] = payload.get("selectedQuestions", []) or []
    fragebogen_list: List[Dict[str, str]] = payload.get("fragebogen", []) or []
    selected_chains: List[str] = payload.get("selectedChains", []) or []

    question_label_by_id: Dict[str, str] = {
        str(q.get("id", "")): str(q.get("label", "") or q.get("id", ""))
        for q in selected_questions
        if q.get("id")
    }
    question_order: List[str] = [str(q.get("id", "")) for q in selected_questions if q.get("id")]

    for row in rows:
        qid = str(row.get("questionId", "")).strip()
        qlabel = str(row.get("questionLabel", "")).strip()
        if qid and qid not in question_label_by_id:
            question_label_by_id[qid] = qlabel or qid
            question_order.append(qid)

    workbook = xlsxwriter.Workbook(str(output_path))

    fmt_header = workbook.add_format({"bold": True, "bg_color": "#D9E1F2", "border": 1})
    fmt_percent = workbook.add_format({"num_format": "0.00%"})
    fmt_note = workbook.add_format({"font_color": "#4B5563"})
    fmt_title = workbook.add_format({"bold": True, "font_size": 14})
    fmt_label = workbook.add_format({"bold": True})

    # ------------------------------------------------------------------
    # RawData
    # ------------------------------------------------------------------
    raw_sheet = workbook.add_worksheet("RawData")
    raw_headers = [
        "MonthKey",
        "Monat",
        "Fragebogen",
        "FrageId",
        "Frage",
        "JaFlag",
        "Antwort",
        "Kunde",
        "Handelskette",
        "AD-Mitarbeiter",
        "ResponseId",
    ]
    raw_sheet.write_row(0, 0, raw_headers, fmt_header)

    raw_rows = sorted(
        rows,
        key=lambda item: (
            str(item.get("monthKey", "")),
            str(item.get("chain", "")),
            str(item.get("questionLabel", "")),
            str(item.get("marketName", "")),
        ),
    )

    for idx, row in enumerate(raw_rows, start=1):
        raw_sheet.write_row(
            idx,
            0,
            [
                str(row.get("monthKey", "")),
                str(row.get("monthLabel", "")),
                str(row.get("fragebogenName", "")),
                str(row.get("questionId", "")),
                str(row.get("questionLabel", "")),
                1 if bool(row.get("answerBoolean", False)) else 0,
                "Ja" if bool(row.get("answerBoolean", False)) else "Nein",
                str(row.get("marketName", "")),
                str(row.get("chain", "")),
                str(row.get("glName", "")),
                str(row.get("responseId", "")),
            ],
        )

    if raw_rows:
        raw_sheet.add_table(
            0,
            0,
            len(raw_rows),
            len(raw_headers) - 1,
            {
                "name": "RawTable",
                "style": "Table Style Medium 2",
                "columns": [{"header": header} for header in raw_headers],
            },
        )
    else:
        raw_sheet.write(
            1,
            0,
            "Keine Daten fuer die ausgewaehlte Kombination gefunden.",
            fmt_note,
        )

    raw_sheet.freeze_panes(1, 0)
    raw_sheet.set_column(0, 0, 12)
    raw_sheet.set_column(1, 1, 11)
    raw_sheet.set_column(2, 2, 28)
    raw_sheet.set_column(3, 3, 36)
    raw_sheet.set_column(4, 4, 45)
    raw_sheet.set_column(5, 6, 10)
    raw_sheet.set_column(7, 7, 28)
    raw_sheet.set_column(8, 8, 20)
    raw_sheet.set_column(9, 9, 24)
    raw_sheet.set_column(10, 10, 40)

    # ------------------------------------------------------------------
    # Aggregation Sheets
    # ------------------------------------------------------------------
    months = unique_months(raw_rows)
    month_key_to_label = {key: label for key, label in months}

    item_month_agg: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(lambda: {"yes": 0, "total": 0})
    customer_agg: Dict[Tuple[str, str, str], Dict[str, int]] = defaultdict(lambda: {"yes": 0, "total": 0})
    ad_agg: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(lambda: {"yes": 0, "total": 0})

    for row in raw_rows:
        month_key = str(row.get("monthKey", ""))
        qid = str(row.get("questionId", ""))
        chain = str(row.get("chain", ""))
        market_name = str(row.get("marketName", ""))
        gl_name = str(row.get("glName", ""))
        yes_flag = 1 if bool(row.get("answerBoolean", False)) else 0

        item_key = (month_key, qid)
        item_month_agg[item_key]["total"] += 1
        item_month_agg[item_key]["yes"] += yes_flag

        customer_key = (month_key, chain, market_name)
        customer_agg[customer_key]["total"] += 1
        customer_agg[customer_key]["yes"] += yes_flag

        ad_key = (month_key, gl_name)
        ad_agg[ad_key]["total"] += 1
        ad_agg[ad_key]["yes"] += yes_flag

    item_sheet = workbook.add_worksheet("ItemDistribution_Monthly")
    item_headers = ["Monat"] + [question_label_by_id.get(qid, qid) for qid in question_order] + ["Alle Items"]
    item_sheet.write_row(0, 0, item_headers, fmt_header)

    for row_index, (month_key, month_label) in enumerate(months, start=1):
        values: List[Any] = [month_label]
        total_yes = 0
        total_count = 0
        for qid in question_order:
            agg = item_month_agg.get((month_key, qid), {"yes": 0, "total": 0})
            yes = agg["yes"]
            total = agg["total"]
            total_yes += yes
            total_count += total
            values.append((yes / total) if total > 0 else 0)
        values.append((total_yes / total_count) if total_count > 0 else 0)
        item_sheet.write_row(row_index, 0, values)

    item_sheet.set_column(0, 0, 12)
    item_sheet.set_column(1, max(1, len(item_headers) - 1), 24, fmt_percent)

    customer_sheet = workbook.add_worksheet("CustomerDistribution_Monthly")
    customer_sheet.write_row(0, 0, ["Monat", "Handelskette", "Kunde", "Ja", "Gesamt", "Distribution"], fmt_header)
    for row_index, (key, agg) in enumerate(sorted(customer_agg.items()), start=1):
        month_key, chain, market_name = key
        total = agg["total"]
        yes = agg["yes"]
        customer_sheet.write_row(
            row_index,
            0,
            [
                month_key_to_label.get(month_key, month_key),
                chain,
                market_name,
                yes,
                total,
                (yes / total) if total > 0 else 0,
            ],
        )
    customer_sheet.set_column(0, 2, 22)
    customer_sheet.set_column(3, 4, 10)
    customer_sheet.set_column(5, 5, 14, fmt_percent)

    ad_sheet = workbook.add_worksheet("ADDistribution_Monthly")
    ad_sheet.write_row(0, 0, ["Monat", "AD-Mitarbeiter", "Ja", "Gesamt", "Distribution"], fmt_header)
    for row_index, (key, agg) in enumerate(sorted(ad_agg.items()), start=1):
        month_key, gl_name = key
        total = agg["total"]
        yes = agg["yes"]
        ad_sheet.write_row(
            row_index,
            0,
            [
                month_key_to_label.get(month_key, month_key),
                gl_name,
                yes,
                total,
                (yes / total) if total > 0 else 0,
            ],
        )
    ad_sheet.set_column(0, 1, 22)
    ad_sheet.set_column(2, 3, 10)
    ad_sheet.set_column(4, 4, 14, fmt_percent)

    # ------------------------------------------------------------------
    # Excel-side selectors + chart
    # ------------------------------------------------------------------
    chains_from_rows = sorted({str(r.get("chain", "")).strip() for r in raw_rows if str(r.get("chain", "")).strip()})
    questions_from_rows = [question_label_by_id.get(qid, qid) for qid in question_order]

    list_sheet = workbook.add_worksheet("Lists")
    list_sheet.hide()
    chain_options = ["Alle"] + chains_from_rows
    question_options = ["Alle"] + questions_from_rows
    for idx, value in enumerate(chain_options):
        list_sheet.write(idx, 0, value)
    for idx, value in enumerate(question_options):
        list_sheet.write(idx, 1, value)

    dashboard = workbook.add_worksheet("Chart")
    dashboard.write("A1", "Fragebogen Distribution (Ja/Nein)", fmt_title)
    dashboard.write(
        "A2",
        "Frageboegen: " + ", ".join([str(fb.get("name", "")) for fb in fragebogen_list if fb.get("name")]),
    )
    dashboard.write("A3", "Handelskette Filter:", fmt_label)
    dashboard.write("A4", "Frage Filter:", fmt_label)
    dashboard.write("B3", "Alle")
    dashboard.write("B4", "Alle")
    dashboard.data_validation("B3", {"validate": "list", "source": f"=Lists!$A$1:$A${len(chain_options)}"})
    dashboard.data_validation("B4", {"validate": "list", "source": f"=Lists!$B$1:$B${len(question_options)}"})
    dashboard.write("A6", "Tipp: Die Filter in B3/B4 steuern die Linie.", fmt_note)
    dashboard.write("A7", "RawData enthaelt zusaetzlich native Tabellenfilter pro Spalte.", fmt_note)
    if selected_chains:
        dashboard.write("A8", "Vorfilter Chains aus App: " + ", ".join(selected_chains), fmt_note)

    chart_data = workbook.add_worksheet("ChartData")
    chart_data.write_row(0, 0, ["MonthKey", "Monat", "Ja", "Gesamt", "Distribution"], fmt_header)

    for row_index, (month_key, month_label) in enumerate(months, start=1):
        excel_row = row_index + 1
        chart_data.write(row_index, 0, month_key)
        chart_data.write(row_index, 1, month_label)
        chart_data.write_formula(
            row_index,
            2,
            (
                f'=SUMIFS(RawData!$F:$F,RawData!$A:$A,$A{excel_row},'
                f'RawData!$I:$I,IF(Chart!$B$3="Alle","<>",Chart!$B$3),'
                f'RawData!$E:$E,IF(Chart!$B$4="Alle","<>",Chart!$B$4))'
            ),
        )
        chart_data.write_formula(
            row_index,
            3,
            (
                f'=COUNTIFS(RawData!$A:$A,$A{excel_row},'
                f'RawData!$I:$I,IF(Chart!$B$3="Alle","<>",Chart!$B$3),'
                f'RawData!$E:$E,IF(Chart!$B$4="Alle","<>",Chart!$B$4))'
            ),
        )
        chart_data.write_formula(
            row_index,
            4,
            f'=IF(D{excel_row}=0,0,C{excel_row}/D{excel_row})',
            fmt_percent,
        )

    chart_data.set_column(0, 1, 12)
    chart_data.set_column(2, 3, 10)
    chart_data.set_column(4, 4, 14, fmt_percent)

    if len(months) > 0:
        line_chart = workbook.add_chart({"type": "line"})
        line_chart.add_series(
            {
                "name": "Distribution",
                "categories": f"=ChartData!$B$2:$B${len(months) + 1}",
                "values": f"=ChartData!$E$2:$E${len(months) + 1}",
                "line": {"color": "#2563EB", "width": 2.0},
                "marker": {"type": "circle", "size": 6},
            }
        )
        line_chart.set_title({"name": "Distribution pro Monat"})
        line_chart.set_x_axis({"name": "Monat"})
        line_chart.set_y_axis({"name": "Distribution", "num_format": "0%"})
        line_chart.set_legend({"none": True})
        dashboard.insert_chart("A10", line_chart, {"x_scale": 1.5, "y_scale": 1.4})

    workbook.close()


def main() -> None:
    if len(sys.argv) != 3:
        die("Usage: fragebogen_distribution_export.py <input_json> <output_xlsx>")

    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])
    output_path.parent.mkdir(parents=True, exist_ok=True)

    payload = parse_input(input_path)
    try:
        build_workbook(payload, output_path)
    except Exception as exc:
        die(f"Failed to build workbook: {exc}")


if __name__ == "__main__":
    main()
