/**
 * Quick User Registration Script
 * Run this to create your first admin and GL users
 * 
 * Usage: npx tsx backend/src/utils/registerUser.ts
 */

import fetch from 'node-fetch';

const API_URL = 'http://localhost:3001/api/auth/register';

async function registerUser(userData: any) {
  try {
    const response = await fetch(API_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(userData),
    });

    const data = await response.json();

    if (!response.ok) {
      console.error('❌ Registration failed:', data.error);
      return false;
    }

    console.log('✅ User registered successfully!');
    console.log(`   Username: ${userData.username}`);
    console.log(`   Role: ${userData.role}`);
    return true;
  } catch (error) {
    console.error('❌ Error:', error);
    return false;
  }
}

async function main() {
  console.log('🚀 Mars Rover User Registration\n');

  // Register Admin
  console.log('Creating admin user...');
  await registerUser({
    username: 'admin',
    password: 'admin123', // Change this!
    email: 'admin@marsrover.com',
    role: 'admin',
    firstName: 'Admin',
    lastName: 'User',
  });

  console.log('');

  // Register GL User
  console.log('Creating GL user...');
  await registerUser({
    username: 'gl_test',
    password: 'gl123', // Change this!
    email: 'gl@marsrover.com',
    role: 'gl',
    firstName: 'Test',
    lastName: 'GL',
    gebietsleiter_id: 'GL001',
  });

  console.log('\n✅ Done! You can now login with:');
  console.log('   Admin: username=admin, password=admin123');
  console.log('   GL: username=gl_test, password=gl123');
}

main();
