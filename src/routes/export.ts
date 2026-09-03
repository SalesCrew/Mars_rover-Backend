import express, { Request, Response } from 'express';
import ExcelJS from 'exceljs';
import { createFreshClient } from '../config/supabase';
import { EXPORT_DATASETS, getColumnDef } from '../config/exportColumns';
import { transformDataset, transformSingleWaveExport } from '../utils/exportTransformers';
import { sendInternalError } from '../utils/httpErrors';

const router = express.Router();

// POST /export/custom
// Generate custom Excel export based on selected datasets and columns
router.post('/custom', async (req: Request, res: Response) => {
  try {
    const {
      datasets,
      columns,
      filters,
      options
    } = req.body;

    // Validate request
    if (!datasets || !Array.isArray(datasets) || datasets.length === 0) {
      return res.status(400).json({ error: 'No datasets selected' });
    }

    if (!columns || typeof columns !== 'object') {
      return res.status(400).json({ error: 'Invalid columns configuration' });
    }

    console.log('📊 Starting Excel export...', {
      datasets: datasets.join(', '),
      fileName: options?.fileName || 'export.xlsx'
    });

    // Create fresh Supabase client
    const freshClient = createFreshClient();

    // Create new workbook
    const workbook = new ExcelJS.Workbook();
    workbook.creator = 'Mars Rover Admin';
    workbook.created = new Date();

    // Handle single wave matrix export
    if (options?.singleWaveExport && options?.singleWaveId) {
      console.log('📊 Single wave matrix export for:', options.singleWaveId);

      const result = await transformSingleWaveExport(freshClient, options.singleWaveId, {
        glIds: filters?.glIds
      });
      const isValueBased = result.goalType === 'value';

      const PASTEL_COLORS = [
        'FFD4E4F7', 'FFFDE2D4', 'FFD4F7E4', 'FFF4D4F7', 'FFF7F0D4',
        'FFD4F7F4', 'FFF7D4D4', 'FFE8D4F7', 'FFD4F0F7', 'FFF7E8D4',
      ];

      const marketHeaders = [
        'Kette',
        'Mars Fil Nr',
        'Interne Markt ID',
        'Markt',
        'Adresse',
        'PLZ',
        'Ort',
        'Gebietsleiter'
      ];
      const itemHeaders = result.items.map(item => {
        const details = [item.name];
        if (item.type === 'einzelprodukt' && item.ve != null && item.ve > 0) {
          details.push(`VE: ${item.ve}`);
        }
        if (item.pricePerUnit > 0) {
          details.push(`Wert/Einheit: €${item.pricePerUnit.toFixed(2)}`);
        }
        return details.join('\n');
      });
      const parentHeaders = result.parentItems.map(parent => (
        `${parent.type === 'palette' ? 'Palette' : 'Schütte'}: ${parent.name}`
      ));
      const totalHeaders = ['Gesamt Menge', 'Gesamt VE'];
      if (isValueBased) totalHeaders.push('Gesamt Wert');
      const headers = [...marketHeaders, ...itemHeaders, ...parentHeaders, ...totalHeaders];
      const metadataColumnCount = marketHeaders.length;
      const firstItemColumn = metadataColumnCount + 1;
      const firstParentColumn = firstItemColumn + result.items.length;
      const totalQuantityColumn = firstParentColumn + result.parentItems.length;
      const totalVeColumn = totalQuantityColumn + 1;
      const totalValueColumn = totalVeColumn + 1;

      const sheet = workbook.addWorksheet(result.waveName, {
        views: [{ state: 'frozen', xSplit: metadataColumnCount, ySplit: 3 }]
      });

      // Row 1: Wave name header
      const titleRow = sheet.addRow([result.waveName]);
      titleRow.font = { bold: true, size: 16 };
      titleRow.height = 28;
      sheet.mergeCells(1, 1, 1, headers.length);

      // Row 2: spacer
      sheet.addRow([]);

      // Row 3: market metadata followed by one quantity column per product
      const headerRowObj = sheet.addRow(headers);
      headerRowObj.font = { bold: true, size: 10 };
      headerRowObj.alignment = { vertical: 'middle', horizontal: 'center', wrapText: true };
      headerRowObj.height = 72;

      marketHeaders.forEach((_, index) => {
        headerRowObj.getCell(index + 1).fill = {
          type: 'pattern',
          pattern: 'solid',
          fgColor: { argb: 'FFE2E8F0' }
        };
      });
      result.items.forEach((item, index) => {
        const colorArgb = item.colorGroup >= 0
          ? PASTEL_COLORS[item.colorGroup % PASTEL_COLORS.length]
          : 'FFF1F5F9';
        headerRowObj.getCell(firstItemColumn + index).fill = {
          type: 'pattern',
          pattern: 'solid',
          fgColor: { argb: colorArgb }
        };
      });
      result.parentItems.forEach((parent, index) => {
        headerRowObj.getCell(firstParentColumn + index).fill = {
          type: 'pattern',
          pattern: 'solid',
          fgColor: { argb: PASTEL_COLORS[parent.colorGroup % PASTEL_COLORS.length] }
        };
      });
      for (let column = totalQuantityColumn; column <= headers.length; column++) {
        headerRowObj.getCell(column).fill = {
          type: 'pattern',
          pattern: 'solid',
          fgColor: { argb: 'FFCBD5E1' }
        };
      }

      // Set column widths
      const metadataWidths = [16, 14, 16, 26, 34, 10, 18, 24];
      metadataWidths.forEach((width, index) => {
        sheet.getColumn(index + 1).width = width;
      });
      for (let i = 0; i < result.items.length; i++) {
        sheet.getColumn(firstItemColumn + i).width = 22;
      }
      for (let i = 0; i < result.parentItems.length; i++) {
        sheet.getColumn(firstParentColumn + i).width = 22;
      }
      sheet.getColumn(totalQuantityColumn).width = 14;
      sheet.getColumn(totalVeColumn).width = 12;
      if (isValueBased) sheet.getColumn(totalValueColumn).width = 14;

      // Row 4+: one market per row, with quantities across product columns
      result.markets.forEach((market, marketIndex) => {
        const itemQuantities = result.items.map(item => result.matrix[item.id]?.[market.id] || 0);
        const parentQuantities = result.parentItems.map(parent => result.parentMatrix[parent.id]?.[market.id] || 0);
        const totalQuantity = itemQuantities.reduce((sum, quantity) => sum + quantity, 0);
        const totalVe = result.items.reduce((sum, item, index) => {
          if (item.type !== 'einzelprodukt' || item.ve == null || item.ve <= 0) return sum;
          return sum + (itemQuantities[index] / item.ve);
        }, 0);
        const totalValue = result.items.reduce(
          (sum, item) => sum + (result.valueMatrix[item.id]?.[market.id] || 0),
          0
        );

        const rowData: any[] = [
          market.chain,
          market.marsFilNr,
          market.internalId,
          market.name,
          market.address,
          market.postalCode,
          market.city,
          market.gebietsleiterName,
          ...itemQuantities.map(quantity => quantity || ''),
          ...parentQuantities.map(quantity => quantity || ''),
          totalQuantity || '',
          totalVe > 0 ? +totalVe.toFixed(2) : '',
        ];
        if (isValueBased) rowData.push(totalValue || '');

        const addedRow = sheet.addRow(rowData);
        addedRow.height = 22;

        if (marketIndex % 2 === 1) {
          for (let column = 1; column <= metadataColumnCount; column++) {
            addedRow.getCell(column).fill = {
              type: 'pattern',
              pattern: 'solid',
              fgColor: { argb: 'FFF8FAFC' }
            };
          }
        }

        result.items.forEach((item, index) => {
          const cell = addedRow.getCell(firstItemColumn + index);
          const colorArgb = item.colorGroup >= 0
            ? PASTEL_COLORS[item.colorGroup % PASTEL_COLORS.length]
            : 'FFF8FAFC';
          cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: colorArgb } };
          cell.alignment = { horizontal: 'center', vertical: 'middle' };
        });

        result.parentItems.forEach((parent, index) => {
          const cell = addedRow.getCell(firstParentColumn + index);
          cell.fill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: PASTEL_COLORS[parent.colorGroup % PASTEL_COLORS.length] }
          };
          cell.alignment = { horizontal: 'center', vertical: 'middle' };
        });

        for (let column = totalQuantityColumn; column <= headers.length; column++) {
          const cell = addedRow.getCell(column);
          cell.font = { bold: true };
          cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF1F5F9' } };
          cell.alignment = { horizontal: 'center', vertical: 'middle' };
        }
        if (isValueBased) addedRow.getCell(totalValueColumn).numFmt = '€#,##0.00';
      });

      // Final totals row keeps the matrix directly usable for comparisons and formulas.
      const itemTotals = result.items.map(item => (
        result.markets.reduce((sum, market) => sum + (result.matrix[item.id]?.[market.id] || 0), 0)
      ));
      const parentTotals = result.parentItems.map(parent => (
        result.markets.reduce((sum, market) => sum + (result.parentMatrix[parent.id]?.[market.id] || 0), 0)
      ));
      const grandQuantity = itemTotals.reduce((sum, quantity) => sum + quantity, 0);
      const grandVe = result.items.reduce((sum, item, index) => {
        if (item.type !== 'einzelprodukt' || item.ve == null || item.ve <= 0) return sum;
        return sum + (itemTotals[index] / item.ve);
      }, 0);
      const grandValue = result.items.reduce((sum, item) => (
        sum + result.markets.reduce(
          (marketSum, market) => marketSum + (result.valueMatrix[item.id]?.[market.id] || 0),
          0
        )
      ), 0);
      const totalRowData: any[] = [
        'Gesamt', '', '', '', '', '', '', '',
        ...itemTotals.map(quantity => quantity || ''),
        ...parentTotals.map(quantity => quantity || ''),
        grandQuantity || '',
        grandVe > 0 ? +grandVe.toFixed(2) : '',
      ];
      if (isValueBased) totalRowData.push(grandValue || '');
      const totalRow = sheet.addRow(totalRowData);
      totalRow.font = { bold: true };
      totalRow.height = 24;
      totalRow.eachCell(cell => {
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFDCE6F1' } };
        cell.alignment = { horizontal: 'center', vertical: 'middle' };
      });
      totalRow.getCell(1).alignment = { horizontal: 'left', vertical: 'middle' };
      if (isValueBased) totalRow.getCell(totalValueColumn).numFmt = '€#,##0.00';

      sheet.autoFilter = {
        from: { row: 3, column: 1 },
        to: { row: 3, column: headers.length }
      };

      // Add borders to all data cells
      const lastRow = sheet.rowCount;
      for (let r = 3; r <= lastRow; r++) {
        const row = sheet.getRow(r);
        for (let c = 1; c <= headers.length; c++) {
          const cell = row.getCell(c);
          cell.border = {
            top: { style: 'thin', color: { argb: 'FFE2E8F0' } },
            bottom: { style: 'thin', color: { argb: 'FFE2E8F0' } },
            left: { style: 'thin', color: { argb: 'FFE2E8F0' } },
            right: { style: 'thin', color: { argb: 'FFE2E8F0' } },
          };
        }
      }

      console.log(`✅ Single wave sheet created: ${result.markets.length} markets × ${result.items.length} items`);

      // Still process other selected datasets (if any besides wellen_submissions)
      const otherDatasets = datasets.filter((id: string) => id !== 'wellen_submissions');
      for (const datasetId of otherDatasets) {
        const datasetDef = EXPORT_DATASETS[datasetId];
        if (!datasetDef) continue;
        const datasetColumns = columns[datasetId] || [];
        if (datasetColumns.length === 0) continue;

        const rows = await transformDataset(freshClient, datasetId, {
          columns: datasetColumns,
          filters: filters || {},
        });

        if (rows.length === 0) continue;

        const otherSheet = workbook.addWorksheet(datasetDef.label, {
          views: [{ state: 'frozen', ySplit: 1 }]
        });

        const otherHeaders: string[] = [];
        const otherCols: Partial<ExcelJS.Column>[] = [];
        datasetColumns.forEach((colId: string) => {
          const colDef = getColumnDef(datasetId, colId);
          if (!colDef) return;
          otherHeaders.push(colDef.label);
          otherCols.push({ key: colId, width: colDef.width || 15 });
        });
        otherSheet.columns = otherCols;

        const otherHeaderRow = otherSheet.addRow(otherHeaders);
        otherHeaderRow.font = { bold: true, size: 11 };
        otherHeaderRow.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFE2E8F0' } };

        rows.forEach((row: any) => {
          const rowData = datasetColumns.map((colId: string) => {
            const value = row[colId];
            if (value === null || value === undefined || value === '') return '';
            const colDef = getColumnDef(datasetId, colId);
            switch (colDef?.type) {
              case 'currency': return typeof value === 'number' ? value : 0;
              case 'number': return typeof value === 'number' ? value : parseFloat(value) || 0;
              case 'datetime': case 'date': return value ? new Date(value) : '';
              default: return String(value);
            }
          });
          otherSheet.addRow(rowData);
        });
      }

      // Skip to file generation
      if (workbook.worksheets.length === 0) {
        return res.status(400).json({ error: 'No data to export' });
      }

      const fileName = options?.fileName || `export_${new Date().toISOString().split('T')[0]}.xlsx`;
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);
      await workbook.xlsx.write(res);
      console.log('✅ Single wave Excel export completed');
      return res.end();
    }

    // Process each dataset
    for (const datasetId of datasets) {
      const datasetDef = EXPORT_DATASETS[datasetId];
      if (!datasetDef) {
        console.warn(`⚠️ Unknown dataset: ${datasetId}, skipping`);
        continue;
      }

      const datasetColumns = columns[datasetId] || [];
      if (datasetColumns.length === 0) {
        console.warn(`⚠️ No columns selected for ${datasetId}, skipping`);
        continue;
      }
      console.log(`📝 Processing dataset: ${datasetDef.label} (${datasetColumns.length} columns)`);

      // Transform data
      const rows = await transformDataset(freshClient, datasetId, {
        columns: datasetColumns,
        filters: filters || {},
        expandPaletteProducts: options?.expandPaletteProducts || false
      });

      if (rows.length === 0) {
        console.log(`  ℹ️ No data for ${datasetId}`);
        continue;
      }

      console.log(`  ✅ Transformed ${rows.length} rows`);

      // Create worksheet
      const sheet = workbook.addWorksheet(datasetDef.label, {
        views: [{ state: 'frozen', ySplit: 1 }] // Freeze header row
      });

      // Prepare headers and column configs
      const headerRow: string[] = [];
      const excelColumns: Partial<ExcelJS.Column>[] = [];

      datasetColumns.forEach((colId: string) => {
        const colDef = getColumnDef(datasetId, colId);
        if (!colDef) return;

        headerRow.push(colDef.label);
        excelColumns.push({
          key: colId,
          width: colDef.width || 15
        });
      });

      sheet.columns = excelColumns;

      // Add and style header row
      const headerRowObj = sheet.addRow(headerRow);
      headerRowObj.font = { bold: true, size: 11 };
      headerRowObj.fill = {
        type: 'pattern',
        pattern: 'solid',
        fgColor: { argb: 'FFE2E8F0' }
      };
      headerRowObj.alignment = { vertical: 'middle', horizontal: 'left' };
      headerRowObj.height = 20;

      // Add data rows with special handling for parent-child structure
      console.log(`  📝 Processing ${rows.length} rows, checking for grouping metadata...`);
      let currentGroupId: string | null = null;
      let groupRowCount = 0;
      let parentCount = 0;
      let childCount = 0;

      rows.forEach((row: any, rowIndex: number) => {
        if (row._isParent) parentCount++;
        if (row._isChild) childCount++;
        
        if (rowIndex < 3) {
          console.log(`  Row ${rowIndex}:`, { 
            isParent: row._isParent, 
            isChild: row._isChild, 
            groupId: row._groupId,
            itemName: row.item_name 
          });
        }
        const rowData = datasetColumns.map((colId: string) => {
          const value = row[colId];
          const colDef = getColumnDef(datasetId, colId);

          // Format based on column type
          if (value === null || value === undefined || value === '') {
            return '';
          }

          switch (colDef?.type) {
            case 'currency':
              return typeof value === 'number' ? value : 0;
            case 'number':
              return typeof value === 'number' ? value : parseFloat(value) || 0;
            case 'datetime':
            case 'date':
              return value ? new Date(value) : '';
            case 'boolean':
              return value;
            default:
              return String(value);
          }
        });

        const addedRow = sheet.addRow(rowData);

        // Track groups for alternating colors
        if (row._groupId !== currentGroupId) {
          currentGroupId = row._groupId || null;
          groupRowCount = 0;
        }
        groupRowCount++;

        // Apply cell formatting based on column types
        datasetColumns.forEach((colId: string, index: number) => {
          const colDef = getColumnDef(datasetId, colId);
          const cell = addedRow.getCell(index + 1);

          // Special formatting for parent rows
          if (row._isParent) {
            cell.font = { bold: true };
            cell.fill = {
              type: 'pattern',
              pattern: 'solid',
              fgColor: { argb: 'FFE0F2FE' } // Light blue for parent
            };
          }

          // Special formatting for child rows
          if (row._isChild) {
            cell.fill = {
              type: 'pattern',
              pattern: 'solid',
              fgColor: { argb: 'FFF0F9FF' } // Very light blue for children
            };
            
            // Add indentation to item_name column
            if (colId === 'item_name') {
              cell.alignment = { horizontal: 'left', indent: 1 };
            }
          }

          // Special formatting for multiline cells (compact mode with product list)
          if (row._isMultiline && colId === 'item_name') {
            cell.alignment = { 
              horizontal: 'left', 
              vertical: 'top',
              wrapText: true 
            };
            cell.font = { size: 10 };
            // Set row height to accommodate multiple lines
            addedRow.height = Math.max(60, Math.min(150, (row.item_name.split('\n').length * 15)));
          }

          switch (colDef?.type) {
            case 'currency':
              cell.numFmt = '€#,##0.00';
              if (!row._isChild || colId === 'value_per_unit' || colId === 'total_value') {
                cell.alignment = { ...cell.alignment, horizontal: 'right' };
              }
              break;
            case 'number':
              cell.numFmt = '#,##0';
              if (!row._isChild) {
                cell.alignment = { ...cell.alignment, horizontal: 'right' };
              }
              break;
            case 'datetime':
              cell.numFmt = 'dd.mm.yyyy hh:mm';
              break;
            case 'date':
              cell.numFmt = 'dd.mm.yyyy';
              break;
            case 'boolean':
              cell.alignment = { ...cell.alignment, horizontal: 'center' };
              break;
          }
        });
      });

      console.log(`  ✅ Created sheet: ${datasetDef.label} (${parentCount} parents, ${childCount} children)`);

      // If this is wellen_submissions, create a separate product details sheet
      if (datasetId === 'wellen_submissions') {
        const allProductDetails: any[] = [];
        rows.forEach((row: any) => {
          if (row._productDetails && Array.isArray(row._productDetails)) {
            row._productDetails.forEach((detail: any) => {
              allProductDetails.push(detail);
            });
          }
        });

        if (allProductDetails.length > 0) {
          console.log(`  📝 Creating Product Details sheet with ${allProductDetails.length} products`);
          
          const detailSheet = workbook.addWorksheet('Produkt Details', {
            views: [{ state: 'frozen', ySplit: 1 }]
          });

          // Define columns for product details
          detailSheet.columns = [
            { key: 'created_at', header: 'Datum', width: 18 },
            { key: 'welle_name', header: 'Welle', width: 25 },
            { key: 'gl_name', header: 'Gebietsleiter', width: 20 },
            { key: 'market_internal_id', header: 'Interne Markt ID', width: 18 },
            { key: 'market_name', header: 'Markt', width: 30 },
            { key: 'market_chain', header: 'Kette', width: 15 },
            { key: 'market_address', header: 'Adresse', width: 30 },
            { key: 'market_postal_code', header: 'PLZ', width: 10 },
            { key: 'market_city', header: 'Stadt', width: 15 },
            { key: 'containerName', header: 'Palette/Schütte', width: 25 },
            { key: 'containerType', header: 'Typ', width: 12 },
            { key: 'productName', header: 'Produkt', width: 40 },
            { key: 'quantity', header: 'Menge', width: 12 },
            { key: 'valuePerUnit', header: 'Wert/Einheit', width: 14 },
            { key: 'totalValue', header: 'Gesamtwert', width: 14 }
          ];

          // Style header row
          const detailHeader = detailSheet.getRow(1);
          detailHeader.font = { bold: true, size: 11 };
          detailHeader.fill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: 'FFE2E8F0' }
          };
          detailHeader.alignment = { vertical: 'middle', horizontal: 'left' };
          detailHeader.height = 20;

          // Sort products by date, then GL, then market, then container
          allProductDetails.sort((a, b) => {
            const dateA = new Date(a.created_at).getTime();
            const dateB = new Date(b.created_at).getTime();
            if (dateA !== dateB) return dateB - dateA; // Newest first
            
            if (a.gl_name !== b.gl_name) return a.gl_name.localeCompare(b.gl_name);
            if (a.market_id !== b.market_id) return String(a.market_id).localeCompare(String(b.market_id));
            return a.containerName.localeCompare(b.containerName);
          });

          // Track groups for separator rows
          let lastContainer = '';
          let lastMarketId = '';

          // Add product rows with separators
          allProductDetails.forEach(detail => {
            // Add separator when container changes within same market
            if (detail.containerName !== lastContainer || detail.market_id !== lastMarketId) {
              const separatorRow = detailSheet.addRow({
                created_at: '',
                welle_name: '',
                gl_name: '',
                market_internal_id: detail.market_internal_id,
                market_name: detail.market_name,
                market_chain: detail.market_chain,
                market_address: detail.market_address,
                market_postal_code: detail.market_postal_code,
                market_city: detail.market_city,
                containerName: `▼ ${detail.containerName}`,
                containerType: detail.containerType,
                productName: '',
                quantity: '',
                valuePerUnit: '',
                totalValue: ''
              });
              separatorRow.font = { bold: true, size: 10 };
              separatorRow.fill = {
                type: 'pattern',
                pattern: 'solid',
                fgColor: { argb: 'FFDBEAFE' }
              };
              
              lastContainer = detail.containerName;
              lastMarketId = detail.market_id;
            }

            // Add product row
            const productRow = detailSheet.addRow({
              created_at: new Date(detail.created_at),
              welle_name: detail.welle_name,
              gl_name: detail.gl_name,
              market_internal_id: detail.market_internal_id,
              market_name: detail.market_name,
              market_chain: detail.market_chain,
              market_address: detail.market_address,
              market_postal_code: detail.market_postal_code,
              market_city: detail.market_city,
              containerName: '',
              containerType: '',
              productName: detail.productName,
              quantity: detail.quantity,
              valuePerUnit: detail.valuePerUnit,
              totalValue: detail.totalValue
            });

            // Format cells
            productRow.getCell(1).numFmt = 'dd.mm.yyyy hh:mm';
            productRow.getCell(12).numFmt = '#,##0';
            productRow.getCell(12).alignment = { horizontal: 'right' };
            productRow.getCell(13).numFmt = '€#,##0.00';
            productRow.getCell(13).alignment = { horizontal: 'right' };
            productRow.getCell(14).numFmt = '€#,##0.00';
            productRow.getCell(14).alignment = { horizontal: 'right' };
          });

          console.log(`  ✅ Created Product Details sheet`);
        }
      }
    }

    if (workbook.worksheets.length === 0) {
      return res.status(400).json({ error: 'No data to export' });
    }

    // Generate Excel file
    const fileName = options?.fileName || `export_${new Date().toISOString().split('T')[0]}.xlsx`;
    
    console.log(`📦 Generating Excel file: ${fileName}`);

    // Set response headers
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);

    // Write to response stream
    await workbook.xlsx.write(res);
    
    console.log('✅ Excel export completed successfully');
    res.end();

  } catch (error: any) {
    console.error('❌ Error generating Excel export:');
    sendInternalError(res);
  }
});

// GET /export/dataset-stats
// Get row counts for all datasets
router.get('/dataset-stats', async (req: Request, res: Response) => {
  try {
    const freshClient = createFreshClient();

    const stats = await Promise.all([
      freshClient.from('wellen_submissions').select('id', { count: 'exact', head: true }),
      freshClient.from('markets').select('id', { count: 'exact', head: true }),
      freshClient.from('vorverkauf_entries').select('id', { count: 'exact', head: true }),
      freshClient.from('action_history').select('id', { count: 'exact', head: true }),
      freshClient.from('gebietsleiter').select('id', { count: 'exact', head: true }).eq('is_active', true)
    ]);

    const result = {
      wellen_submissions: stats[0].count || 0,
      markets: stats[1].count || 0,
      vorverkauf_entries: stats[2].count || 0,
      action_history: stats[3].count || 0,
      gebietsleiter: stats[4].count || 0
    };

    res.json(result);
  } catch (error: any) {
    console.error('Error fetching dataset stats:');
    sendInternalError(res);
  }
});

export default router;
