import assert from 'node:assert/strict';
import { test } from 'node:test';
import exportColumns from '../src/config/exportColumns.ts';
import exportTransformers from '../src/utils/exportTransformers.ts';

const { getDefaultColumns } = exportColumns;
const { transformMarkets } = exportTransformers;

test('market visit export includes the customer banner by default and reads its source field', async () => {
  assert.ok(getDefaultColumns('markets').includes('banner'));

  const selections = [];
  const client = {
    from(table) {
      assert.equal(table, 'markets');
      return {
        select(columns) {
          selections.push(columns);
          return {
            order() {
              return {
                async range(from) {
                  return {
                    data: from === 0 ? [{
                      id: 'market-1', name: 'Spar Test', chain: 'Spar',
                      banner: 'SPAR-Spar SM Privat', frequency: 12, current_visits: 3,
                      is_active: true,
                    }] : [],
                    error: null,
                  };
                },
              };
            },
          };
        },
      };
    },
  };

  const rows = await transformMarkets(client, {
    columns: ['id', 'chain', 'banner', 'frequency', 'current_visits'],
    filters: {},
  });
  assert.ok(selections[0].split(',').map(column => column.trim()).includes('banner'));
  assert.deepEqual(rows, [{
    id: 'market-1', chain: 'Spar', banner: 'SPAR-Spar SM Privat',
    frequency: 12, current_visits: 3,
  }]);
});
