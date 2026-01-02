/**
 * Password Hashing Utility
 * Use this script to generate bcrypt hashes for user passwords
 * 
 * Usage in backend terminal:
 *   npx tsx backend/src/utils/hashPassword.ts <password>
 */

import bcrypt from 'bcrypt';

const password = process.argv[2];

if (!password) {
  console.error('❌ Error: Please provide a password');
  console.log('\nUsage: npx tsx backend/src/utils/hashPassword.ts <password>');
  console.log('Example: npx tsx backend/src/utils/hashPassword.ts password123\n');
  process.exit(1);
}

async function hashPassword(plainPassword: string) {
  try {
    const hash = await bcrypt.hash(plainPassword, 10);
    console.log('\n✅ Password hashed successfully!');
    console.log(`\nPassword: ${plainPassword}`);
    console.log(`Hash: ${hash}\n`);
    console.log('Copy this hash to your SQL INSERT statement or use the /api/auth/register endpoint.\n');
  } catch (error) {
    console.error('❌ Error hashing password:', error);
    process.exit(1);
  }
}

hashPassword(password);
