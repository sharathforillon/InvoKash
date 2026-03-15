/**
 * InvoKash v2 — Entry Point
 * Starts Telegram bot (polling) + WhatsApp webhook server (Express) + Scheduler
 */

require('dotenv').config();

// ─── Validate Required Env Vars ───────────────────────────────────────────────
const required = ['ANTHROPIC_API_KEY'];
const missing  = required.filter(k => !process.env[k]);
if (missing.length) {
  console.error(`❌ Missing required environment variables: ${missing.join(', ')}`);
  console.error('   Copy .env.example to .env and fill in your values.');
  process.exit(1);
}

// Soft warnings for optional but important vars
if (!process.env.TELEGRAM_TOKEN)        console.warn('⚠️  TELEGRAM_TOKEN not set — Telegram bot disabled.');
if (!process.env.OPENAI_API_KEY)        console.warn('⚠️  OPENAI_API_KEY not set — Voice transcription disabled.');
if (!process.env.STRIPE_SECRET_KEY)     console.warn('⚠️  STRIPE_SECRET_KEY not set — Payment links disabled.');
if (!process.env.WHATSAPP_TOKEN)        console.warn('⚠️  WHATSAPP_TOKEN not set — WhatsApp disabled.');
if (!process.env.STRIPE_WEBHOOK_SECRET) console.warn('⚠️  STRIPE_WEBHOOK_SECRET not set — Stripe auto-paid notifications disabled.');

console.log('🚀 InvoKash v2 starting...');

// ─── Start Platforms ──────────────────────────────────────────────────────────
let telegramNotify = null;
let waSend         = null;

if (process.env.TELEGRAM_TOKEN) {
  const botModule = require('./bot');
  botModule.startTelegramBot();
  telegramNotify = botModule.telegramNotify;
}

if (process.env.WHATSAPP_TOKEN && process.env.WHATSAPP_PHONE_ID) {
  const waModule = require('./whatsapp');
  waModule.startWhatsAppServer();
  waSend = waModule.waSend;
}

// ─── Start Overdue Reminder Scheduler ─────────────────────────────────────────
const { initScheduler } = require('./scheduler');
initScheduler(telegramNotify, waSend);

// ─── Ready ────────────────────────────────────────────────────────────────────
console.log('✅ InvoKash v2 ready!');
console.log('   Platforms:', [
  process.env.TELEGRAM_TOKEN   ? '📱 Telegram' : null,
  process.env.WHATSAPP_TOKEN   ? '💬 WhatsApp' : null,
].filter(Boolean).join(' + ') || 'None configured');
console.log('   Features: AI invoicing · PDF · Payments · Aging · Goals · Templates · Expenses · Scheduler');
