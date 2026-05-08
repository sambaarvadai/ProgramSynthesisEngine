import { initPeeStore } from '../storage/initPeeStore.js';
import { connectPeeStore } from '../storage/PeeStoreBackend.js';
import * as dotenv from 'dotenv';
dotenv.config();

async function init() {
  try {
    console.log('Connecting to pee_store...');
    await connectPeeStore();
    console.log('Initializing pee_store schema...');
    await initPeeStore();
    console.log('pee_store schema initialized successfully');
  } catch (e) {
    console.error('Error initializing pee_store:', e);
  }
}

init().catch(console.error);
