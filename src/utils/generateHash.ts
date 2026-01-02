import bcrypt from 'bcrypt';

async function generateHash() {
  const password = 'test123';
  const hash = await bcrypt.hash(password, 10);
  console.log('\n===========================================');
  console.log('✅ Password Hash Generated!');
  console.log('===========================================');
  console.log(`Password: ${password}`);
  console.log(`Hash: ${hash}`);
  console.log('\nCopy the SQL below and run it in Supabase SQL Editor:\n');
  console.log(`UPDATE users SET password_hash = '${hash}' WHERE username = 'Kilian Test GL';`);
  console.log('\n===========================================\n');
}

generateHash();
