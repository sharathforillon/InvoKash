require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const axios = require('axios');
const PDFDocument = require('pdfkit');
const fs = require('fs');
const path = require('path');
const { createPaymentLink } = require('./payments');
const OpenAI = require('openai');
const archiver = require('archiver');

// ─── Environment Validation ───────────────────────────────────────────────────
const TELEGRAM_TOKEN   = process.env.TELEGRAM_TOKEN;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const OPENAI_API_KEY   = process.env.OPENAI_API_KEY;

if (!TELEGRAM_TOKEN || !ANTHROPIC_API_KEY || !OPENAI_API_KEY) {
  console.error('ERROR: Missing required environment variables (TELEGRAM_TOKEN, ANTHROPIC_API_KEY, OPENAI_API_KEY)');
  process.exit(1);
}

const bot    = new TelegramBot(TELEGRAM_TOKEN, { polling: true });
const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

// ─── Paths ────────────────────────────────────────────────────────────────────
const BASE_DIR      = __dirname;
const INVOICE_DIR   = path.join(BASE_DIR, 'invoices');
const DATA_DIR      = path.join(BASE_DIR, 'data');
const PROFILES_FILE = path.join(DATA_DIR, 'profiles.json');
const HISTORY_FILE  = path.join(DATA_DIR, 'history.json');
const BACKUP_DIR    = path.join(BASE_DIR, 'backups');

['/tmp/logos', INVOICE_DIR, DATA_DIR, BACKUP_DIR].forEach(dir => {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

// ─── In-Memory State ──────────────────────────────────────────────────────────
let companyProfiles  = {};
let invoiceHistory   = {};
let onboardingState  = {};   // { userId: { step, ... } }
let commandState     = {};   // { userId: { type } }
let pendingInvoices  = {};   // { userId: invoiceData } — awaiting user confirmation

const userRateLimits = new Map();

// ─── Currency Config ──────────────────────────────────────────────────────────
const CURRENCIES = {
  AED: { symbol: 'AED', flag: '🇦🇪', name: 'UAE Dirham',      tax: 'VAT', right: true },
  USD: { symbol: '$',   flag: '🇺🇸', name: 'US Dollar',        tax: 'VAT', right: false },
  EUR: { symbol: '€',   flag: '🇪🇺', name: 'Euro',             tax: 'VAT', right: false },
  INR: { symbol: '₹',   flag: '🇮🇳', name: 'Indian Rupee',     tax: 'GST', right: false },
  SAR: { symbol: 'SAR', flag: '🇸🇦', name: 'Saudi Riyal',      tax: 'VAT', right: true  },
  GBP: { symbol: '£',   flag: '🇬🇧', name: 'British Pound',    tax: 'VAT', right: false },
};

// ─── Data Persistence ─────────────────────────────────────────────────────────
function loadData() {
  try {
    if (fs.existsSync(PROFILES_FILE)) companyProfiles = JSON.parse(fs.readFileSync(PROFILES_FILE, 'utf8'));
    if (fs.existsSync(HISTORY_FILE))  invoiceHistory  = JSON.parse(fs.readFileSync(HISTORY_FILE,  'utf8'));
  } catch (err) { console.error('Load error:', err.message); }
}

function saveData() {
  try {
    fs.writeFileSync(PROFILES_FILE, JSON.stringify(companyProfiles, null, 2));
    fs.writeFileSync(HISTORY_FILE,  JSON.stringify(invoiceHistory,  null, 2));
  } catch (err) { console.error('Save error:', err.message); }
}

loadData();
console.log('InvoKash Bot starting...');

// ─── Helpers ──────────────────────────────────────────────────────────────────
function checkRateLimit(userId) {
  const now   = Date.now();
  const limit = userRateLimits.get(userId) || { count: 0, resetTime: now + 60000 };
  if (now > limit.resetTime) { limit.count = 0; limit.resetTime = now + 60000; }
  limit.count++;
  userRateLimits.set(userId, limit);
  return limit.count <= 20;
}

function sanitizeInput(input) {
  if (typeof input !== 'string') return '';
  return input.replace(/[<>]/g, '').trim().slice(0, 500);
}

function formatAmount(amount, currency) {
  const cfg = CURRENCIES[currency];
  const num = parseFloat(amount).toFixed(2);
  if (!cfg) return `${currency} ${num}`;
  return cfg.right ? `${num} ${cfg.symbol}` : `${cfg.symbol}${num}`;
}

function progressBar(step, total) {
  const filled = Math.round((step / total) * 8);
  return `[${'█'.repeat(filled)}${'░'.repeat(8 - filled)}] Step ${step}/${total}`;
}

function getTaxConfig(profile) {
  if (profile.currency === 'INR') {
    return { enabled: !!profile.gst_enabled, rate: profile.gst_rate || 0, type: 'GST', field: 'gst' };
  }
  return { enabled: !!profile.vat_enabled, rate: profile.vat_rate || 0, type: 'VAT', field: 'vat' };
}

// ─── Main Message Handler ─────────────────────────────────────────────────────
bot.on('message', async (msg) => {
  const chatId    = msg.chat.id;
  const userId    = String(msg.from.id);
  const text      = msg.text;
  const firstName = msg.from.first_name || 'there';

  if (!checkRateLimit(userId)) {
    bot.sendMessage(chatId, '⏱ Too many requests — please wait a moment and try again.');
    return;
  }

  try {
    // Commands
    if (text && text.startsWith('/')) {
      await handleCommand(chatId, userId, text, firstName);
      return;
    }

    // Logo upload during onboarding
    if (msg.photo && onboardingState[userId]?.step === 'logo') {
      await handleLogoUpload(chatId, userId, msg.photo);
      return;
    }

    // Not set up yet
    if (!companyProfiles[userId] && !onboardingState[userId]) {
      await bot.sendMessage(chatId,
        `👋 Hi *${firstName}*! I'm *InvoKash* — your AI invoice assistant.\n\n` +
        `Create professional invoices in seconds using voice or text, and collect payments instantly.\n\n` +
        `Let's get your business set up first!`,
        {
          parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: [[
            { text: '🚀 Set Up My Account', callback_data: 'cmd_setup' },
            { text: '❓ How It Works',       callback_data: 'cmd_help'  }
          ]]}
        }
      );
      return;
    }

    // Voice message
    if (msg.voice) {
      await handleVoiceMessage(chatId, userId, msg.voice, firstName);
      return;
    }

    // Active onboarding flow
    if (onboardingState[userId]) {
      await handleOnboarding(chatId, userId, text);
      return;
    }

    // Active command state (period selection via text)
    if (commandState[userId]) {
      await handleCommandState(chatId, userId, text);
      return;
    }

    // Free-text invoice / command
    if (text) {
      await handleTextMessage(chatId, userId, text, firstName);
    }

  } catch (err) {
    console.error('Message handler error:', err);
    bot.sendMessage(chatId, '⚠️ Something went wrong. Please try again or use /help.');
  }
});

// ─── Callback Query Handler (Inline Keyboards) ────────────────────────────────
bot.on('callback_query', async (query) => {
  const chatId    = query.message.chat.id;
  const userId    = String(query.from.id);
  const data      = query.data;
  const firstName = query.from.first_name || 'there';

  await bot.answerCallbackQuery(query.id);

  if (!checkRateLimit(userId)) return;

  try {
    // ── Setup actions ──
    if      (data === 'cmd_setup')    { startOnboarding(chatId, userId, firstName); }
    else if (data === 'cmd_help')     { showHelp(chatId); }
    else if (data === 'setup_agree')  { await handleOnboarding(chatId, userId, 'agree'); }
    else if (data === 'setup_cancel') {
      delete onboardingState[userId];
      bot.sendMessage(chatId, '❌ Setup cancelled. Use /setup to start again any time.');
    }
    else if (data === 'setup_skip')   { await handleOnboarding(chatId, userId, 'skip'); }
    else if (data.startsWith('currency_')) {
      await handleOnboarding(chatId, userId, data.replace('currency_', ''));
    }
    else if (data === 'tax_yes')      { await handleOnboarding(chatId, userId, 'yes'); }
    else if (data === 'tax_no')       { await handleOnboarding(chatId, userId, 'no'); }

    // ── Stats / Download period selection ──
    else if (data.startsWith('stats_')) {
      delete commandState[userId];
      await showStats(chatId, userId, data.replace('stats_', ''));
    }
    else if (data.startsWith('dl_')) {
      delete commandState[userId];
      await downloadInvoicesByPeriod(chatId, userId, data.replace('dl_', ''));
    }

    // ── Invoice confirmation ──
    else if (data === 'confirm_invoice') { await confirmAndGenerateInvoice(chatId, userId); }
    else if (data === 'retry_invoice') {
      delete pendingInvoices[userId];
      bot.sendMessage(chatId,
        '🔄 No problem! Please re-describe your invoice.\n\n' +
        'Example: _"Web design for Acme Corp for 3000"_',
        { parse_mode: 'Markdown' }
      );
    }

    // ── Navigation shortcuts ──
    else if (data === 'nav_invoices') { await showInvoiceHistory(chatId, userId); }
    else if (data === 'nav_stats')    { await showPeriodSelector(chatId, userId, 'stats'); }
    else if (data === 'nav_profile')  { showProfile(chatId, userId); }
    else if (data === 'nav_download') { await showPeriodSelector(chatId, userId, 'download'); }
    else if (data === 'nav_home')     { showWelcomeMessage(chatId, userId, firstName); }

    // ── Delete data ──
    else if (data === 'deletedata_confirm') {
      delete companyProfiles[userId];
      delete invoiceHistory[userId];
      saveData();
      bot.sendMessage(chatId, '🗑 All your data has been permanently deleted.\n\nUse /setup to start fresh any time.');
    }
    else if (data === 'deletedata_cancel') {
      bot.sendMessage(chatId, '✅ Your data is safe. Nothing was deleted.');
    }

  } catch (err) {
    console.error('Callback query error:', err);
    bot.sendMessage(chatId, '⚠️ Something went wrong. Please try again.');
  }
});

// ─── Command Handler ──────────────────────────────────────────────────────────
async function handleCommand(chatId, userId, command, firstName) {
  const cmd = command.split(' ')[0].toLowerCase();

  switch (cmd) {
    case '/start':      showWelcomeMessage(chatId, userId, firstName);               break;
    case '/setup':      startOnboarding(chatId, userId, firstName);                  break;
    case '/help':       showHelp(chatId);                                             break;
    case '/profile':    showProfile(chatId, userId);                                  break;
    case '/invoices':   await showInvoiceHistory(chatId, userId);                    break;
    case '/customers':  showCustomers(chatId, userId);                               break;
    case '/stats':      await showPeriodSelector(chatId, userId, 'stats');           break;
    case '/download':   await showPeriodSelector(chatId, userId, 'download');        break;
    case '/deletedata': await confirmDeleteData(chatId, userId);                     break;
    case '/agree':
      if (onboardingState[userId]) await handleOnboarding(chatId, userId, 'agree');
      break;
    case '/cancel':
      if (onboardingState[userId]) { delete onboardingState[userId]; bot.sendMessage(chatId, '❌ Setup cancelled.'); }
      else if (commandState[userId]) { delete commandState[userId]; bot.sendMessage(chatId, '❌ Cancelled.'); }
      break;
    case '/skip':
      if (onboardingState[userId]) await handleOnboarding(chatId, userId, 'skip');
      break;
    default:
      // Legacy period commands
      if (['/this_month','/last_month','/this_quarter','/this_year','/all'].includes(cmd)) {
        const period = cmd.slice(1);
        const state  = commandState[userId];
        delete commandState[userId];
        if (state?.type === 'stats') await showStats(chatId, userId, period);
        else await downloadInvoicesByPeriod(chatId, userId, period);
      }
  }
}

// ─── Welcome Screen ───────────────────────────────────────────────────────────
function showWelcomeMessage(chatId, userId, firstName = 'there') {
  const profile = companyProfiles[userId];
  const history = invoiceHistory[userId] || [];

  if (profile) {
    const thisMonth  = filterInvoicesByPeriod(history, 'this_month');
    const monthTotal = thisMonth.reduce((s, i) => s + (parseFloat(i.total) || 0), 0);

    let msg = `👋 Welcome back, *${profile.company_name}*!\n\n`;

    if (thisMonth.length > 0) {
      msg += `📊 *This Month*\n`;
      msg += `📄 ${thisMonth.length} invoice${thisMonth.length !== 1 ? 's' : ''} · `;
      msg += `💰 ${formatAmount(monthTotal, profile.currency)}\n\n`;
    }

    msg += `🎤 *Create an Invoice*\n`;
    msg += `Just type or send a voice message:\n`;
    msg += `_"Plumbing for Ahmed at Marina for 500"_\n`;
    msg += `_"Consulting for Acme Corp for 3000"_\n\n`;
    msg += `📱 /invoices · /stats · /download · /profile`;

    bot.sendMessage(chatId, msg, {
      parse_mode: 'Markdown',
      reply_markup: { inline_keyboard: [[
        { text: '📊 Stats',    callback_data: 'nav_stats'    },
        { text: '📋 Invoices', callback_data: 'nav_invoices' },
        { text: '👤 Profile',  callback_data: 'nav_profile'  },
      ]]}
    });

  } else {
    bot.sendMessage(chatId,
      `👋 Welcome to *InvoKash*, ${firstName}!\n\n` +
      `🚀 Create professional invoices in seconds — built for freelancers & SMEs across the Middle East, South Asia, and beyond.\n\n` +
      `✅ Voice & text invoice creation\n` +
      `✅ VAT / GST compliant\n` +
      `✅ Instant Stripe payment links\n` +
      `✅ PDF delivery on Telegram\n` +
      `✅ Multi-currency: AED · USD · EUR · INR · SAR · GBP\n\n` +
      `Let's get you set up in 2 minutes!`,
      {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[
          { text: '🚀 Set Up My Account', callback_data: 'cmd_setup' }
        ]]}
      }
    );
  }
}

// ─── Help ─────────────────────────────────────────────────────────────────────
function showHelp(chatId) {
  bot.sendMessage(chatId,
    `📖 *InvoKash Help*\n\n` +
    `*Creating Invoices*\nType or send a voice message:\n` +
    `• _"Plumbing for Ahmed at Marina for 500"_\n` +
    `• _"Web design for Acme Corp for 3000"_\n` +
    `• _"Consulting, design, support for TechCo for 2500"_\n\n` +
    `*Commands*\n` +
    `/start — Home screen\n` +
    `/setup — Set up or update your profile\n` +
    `/profile — View your business profile\n` +
    `/invoices — Recent invoices\n` +
    `/customers — Customer list\n` +
    `/stats — Revenue statistics\n` +
    `/download — Download invoices (PDF + CSV)\n` +
    `/deletedata — Delete all your data\n` +
    `/help — This guide\n\n` +
    `*Tips*\n` +
    `• Always include customer name, service, and amount\n` +
    `• Location is optional\n` +
    `• Multiple services in one invoice: _"Design and hosting for Client for 2000"_\n\n` +
    `*Support* — @InvoKashSupport`,
    { parse_mode: 'Markdown' }
  );
}

// ─── Onboarding ───────────────────────────────────────────────────────────────
const ONBOARD_TOTAL = 10;

function startOnboarding(chatId, userId, firstName = 'there') {
  const isUpdate = !!companyProfiles[userId];
  onboardingState[userId] = { step: 'disclaimer' };

  bot.sendMessage(chatId,
    (isUpdate
      ? `⚙️ *Update Your Profile*\n\nThis will replace your current settings.\n\n`
      : `🎉 *Let's set up your account, ${firstName}!*\n\n`) +
    `⚠️ *Disclaimer*\n\n` +
    `InvoKash generates invoices for *record-keeping purposes only*. These are not legally certified tax documents.\n\n` +
    `By proceeding you confirm:\n` +
    `• You are responsible for tax compliance in your country\n` +
    `• Your data is stored securely and never shared\n` +
    `• You can delete your data any time with /deletedata`,
    {
      parse_mode: 'Markdown',
      reply_markup: { inline_keyboard: [[
        { text: '✅ I Agree — Continue', callback_data: 'setup_agree'  },
        { text: '❌ Cancel',             callback_data: 'setup_cancel' }
      ]]}
    }
  );
}

async function handleLogoUpload(chatId, userId, photos) {
  try {
    const photo   = photos[photos.length - 1];
    const file    = await bot.getFile(photo.file_id);
    const fileUrl = `https://api.telegram.org/file/bot${TELEGRAM_TOKEN}/${file.file_path}`;
    const res     = await axios.get(fileUrl, { responseType: 'arraybuffer' });
    const logoPath = `/tmp/logos/logo_${userId}.jpg`;
    fs.writeFileSync(logoPath, Buffer.from(res.data));
    companyProfiles[userId].logo_path = logoPath;
  } catch (err) {
    console.error('Logo upload error:', err.message);
    companyProfiles[userId].logo_path = null;
  }
  delete onboardingState[userId];
  saveData();
  await sendSetupComplete(chatId, userId);
}

async function handleOnboarding(chatId, userId, text) {
  const state = onboardingState[userId];
  if (!state) return;

  if (!companyProfiles[userId]) companyProfiles[userId] = {};
  const input = (text || '').toLowerCase().trim();
  const p     = companyProfiles[userId];

  // ── disclaimer ──────────────────────────────────────────────────────────────
  if (state.step === 'disclaimer') {
    if (input !== 'agree') return;  // only explicit /agree or callback proceeds
    onboardingState[userId].step = 'company_name';
    bot.sendMessage(chatId,
      `${progressBar(1, ONBOARD_TOTAL)}\n\n` +
      `🏢 *Company Name*\n\nWhat's your business or company name?`,
      { parse_mode: 'Markdown' }
    );

  // ── company_name ────────────────────────────────────────────────────────────
  } else if (state.step === 'company_name') {
    if (!text || text.trim().length < 1) {
      bot.sendMessage(chatId, '⚠️ Please enter a valid company name.');
      return;
    }
    p.company_name = sanitizeInput(text);
    onboardingState[userId].step = 'company_address';
    bot.sendMessage(chatId,
      `${progressBar(2, ONBOARD_TOTAL)}\n\n` +
      `📍 *Business Address*\n\nEnter your full business address:`,
      { parse_mode: 'Markdown' }
    );

  // ── company_address ─────────────────────────────────────────────────────────
  } else if (state.step === 'company_address') {
    p.company_address = sanitizeInput(text);
    onboardingState[userId].step = 'trn';
    bot.sendMessage(chatId,
      `${progressBar(3, ONBOARD_TOTAL)}\n\n` +
      `🔐 *Tax Registration Number (TRN)*\n\nEnter your TRN/VAT/GST registration number, or skip if you don't have one.`,
      {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[{ text: '⏭ Skip', callback_data: 'setup_skip' }]] }
      }
    );

  // ── trn ─────────────────────────────────────────────────────────────────────
  } else if (state.step === 'trn') {
    p.trn = input === 'skip' ? '' : sanitizeInput(text);
    onboardingState[userId].step = 'currency';
    bot.sendMessage(chatId,
      `${progressBar(4, ONBOARD_TOTAL)}\n\n` +
      `💰 *Invoice Currency*\n\nSelect the currency you invoice in:`,
      {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [
          [
            { text: '🇦🇪 AED', callback_data: 'currency_AED' },
            { text: '🇺🇸 USD', callback_data: 'currency_USD' },
            { text: '🇪🇺 EUR', callback_data: 'currency_EUR' },
          ],
          [
            { text: '🇮🇳 INR', callback_data: 'currency_INR' },
            { text: '🇸🇦 SAR', callback_data: 'currency_SAR' },
            { text: '🇬🇧 GBP', callback_data: 'currency_GBP' },
          ]
        ]}
      }
    );

  // ── currency ─────────────────────────────────────────────────────────────────
  } else if (state.step === 'currency') {
    const curr = input.toUpperCase();
    if (!CURRENCIES[curr]) {
      bot.sendMessage(chatId, '⚠️ Please select a currency using the buttons above.');
      return;
    }
    p.currency = curr;
    onboardingState[userId].step = 'bank_name';
    bot.sendMessage(chatId,
      `${progressBar(5, ONBOARD_TOTAL)}\n\n` +
      `🏦 *Bank Name*\n\nEnter your bank name (e.g., Emirates NBD, HDFC, Chase, Lloyds):`,
      { parse_mode: 'Markdown' }
    );

  // ── bank_name ────────────────────────────────────────────────────────────────
  } else if (state.step === 'bank_name') {
    p.bank_name = sanitizeInput(text);
    onboardingState[userId].step = 'iban';
    const label = p.currency === 'INR' ? 'Account Number & IFSC (e.g., 123456789 / HDFC0001234)' : 'IBAN';
    bot.sendMessage(chatId,
      `${progressBar(6, ONBOARD_TOTAL)}\n\n` +
      `🔑 *${p.currency === 'INR' ? 'Account Number & IFSC' : 'IBAN'}*\n\nEnter your ${label}:`,
      { parse_mode: 'Markdown' }
    );

  // ── iban ─────────────────────────────────────────────────────────────────────
  } else if (state.step === 'iban') {
    p.iban = sanitizeInput(text);
    onboardingState[userId].step = 'account_name';
    bot.sendMessage(chatId,
      `${progressBar(7, ONBOARD_TOTAL)}\n\n` +
      `👤 *Account Holder Name*\n\nEnter the name on the bank account:`,
      { parse_mode: 'Markdown' }
    );

  // ── account_name ─────────────────────────────────────────────────────────────
  } else if (state.step === 'account_name') {
    p.account_name = sanitizeInput(text);
    onboardingState[userId].step = 'tax_enabled';
    const taxType = CURRENCIES[p.currency]?.tax || 'VAT';
    bot.sendMessage(chatId,
      `${progressBar(8, ONBOARD_TOTAL)}\n\n` +
      `📊 *Tax Settings*\n\nDo you charge *${taxType}* on your invoices?`,
      {
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[
          { text: `✅ Yes, I charge ${taxType}`, callback_data: 'tax_yes' },
          { text: '❌ No tax',                   callback_data: 'tax_no'  }
        ]]}
      }
    );

  // ── tax_enabled ──────────────────────────────────────────────────────────────
  } else if (state.step === 'tax_enabled') {
    const taxField = p.currency === 'INR' ? 'gst' : 'vat';
    if (input === 'yes') {
      p[`${taxField}_enabled`] = true;
      if (taxField === 'gst') { p.vat_enabled = false; p.vat_rate = 0; }
      else                    { p.gst_enabled = false; p.gst_rate = 0; }
      onboardingState[userId].step = 'tax_rate';
      bot.sendMessage(chatId,
        `${progressBar(9, ONBOARD_TOTAL)}\n\n` +
        `📈 *${taxField.toUpperCase()} Rate*\n\nEnter the ${taxField.toUpperCase()} percentage (e.g., \`5\` for 5%):`,
        { parse_mode: 'Markdown' }
      );
    } else if (input === 'no') {
      p.vat_enabled = false; p.vat_rate = 0;
      p.gst_enabled = false; p.gst_rate = 0;
      onboardingState[userId].step = 'logo';
      await sendLogoPrompt(chatId);
    } else {
      bot.sendMessage(chatId, '⚠️ Please tap ✅ Yes or ❌ No using the buttons above.');
    }

  // ── tax_rate ─────────────────────────────────────────────────────────────────
  } else if (state.step === 'tax_rate') {
    const rate = parseFloat(text);
    if (isNaN(rate) || rate < 0 || rate > 100) {
      bot.sendMessage(chatId, '⚠️ Please enter a valid percentage between 0 and 100 (e.g., 5).');
      return;
    }
    const taxField = p.currency === 'INR' ? 'gst' : 'vat';
    p[`${taxField}_rate`] = rate;
    onboardingState[userId].step = 'logo';
    await sendLogoPrompt(chatId);

  // ── logo ──────────────────────────────────────────────────────────────────────
  } else if (state.step === 'logo') {
    if (input === 'skip') {
      p.logo_path = null;
      delete onboardingState[userId];
      saveData();
      await sendSetupComplete(chatId, userId);
    }
  }
}

async function sendLogoPrompt(chatId) {
  await bot.sendMessage(chatId,
    `${progressBar(10, ONBOARD_TOTAL)}\n\n` +
    `🖼 *Company Logo (Optional)*\n\nSend your logo image (PNG or JPG), or skip to use text only.`,
    {
      parse_mode: 'Markdown',
      reply_markup: { inline_keyboard: [[{ text: '⏭ Skip Logo', callback_data: 'setup_skip' }]] }
    }
  );
}

async function sendSetupComplete(chatId, userId) {
  const p = companyProfiles[userId];
  const tc = getTaxConfig(p);
  const curr = CURRENCIES[p.currency] || {};

  await bot.sendMessage(chatId,
    `🎉 *Setup Complete!*\n\n` +
    `Here's your business profile:\n\n` +
    `🏢 *${p.company_name}*\n` +
    `📍 ${p.company_address}\n` +
    `${curr.flag || ''} ${p.currency} (${curr.name || ''})\n` +
    `📊 ${tc.type}: ${tc.enabled ? `${tc.rate}%` : 'Not charged'}\n` +
    `🏦 ${p.bank_name || 'No bank set'}\n` +
    `🖼 Logo: ${p.logo_path ? '✅' : '❌'}\n\n` +
    `You're all set! Just type or send a voice message to create your first invoice.\n\n` +
    `_Example: "Web design for Acme Corp for 3000"_`,
    {
      parse_mode: 'Markdown',
      reply_markup: { inline_keyboard: [[
        { text: '📖 How to Create Invoices', callback_data: 'cmd_help' }
      ]]}
    }
  );
}

// ─── Profile Display ──────────────────────────────────────────────────────────
function showProfile(chatId, userId) {
  const p = companyProfiles[userId];
  if (!p) {
    bot.sendMessage(chatId, '👤 No profile found.\n\nUse /setup to create your business profile.', {
      reply_markup: { inline_keyboard: [[{ text: '🚀 Set Up Now', callback_data: 'cmd_setup' }]] }
    });
    return;
  }

  const curr = CURRENCIES[p.currency] || {};
  const tc   = getTaxConfig(p);
  const invs = invoiceHistory[userId] || [];

  bot.sendMessage(chatId,
    `👤 *Business Profile*\n━━━━━━━━━━━━━━━━━━━━━━\n\n` +
    `🏢 *${p.company_name}*\n` +
    `📍 ${p.company_address}\n` +
    `${p.trn ? `🔐 TRN: \`${p.trn}\`` : '🔐 TRN: _Not set_'}\n\n` +
    `💰 *Invoice Settings*\n` +
    `${curr.flag || ''} Currency: *${p.currency}* — ${curr.name || ''}\n` +
    `📊 ${tc.type}: ${tc.enabled ? `✅ ${tc.rate}%` : '❌ Not charged'}\n\n` +
    `🏦 *Bank Details*\n` +
    `🏛 ${p.bank_name    || '_Not set_'}\n` +
    `🔑 ${p.iban         || '_Not set_'}\n` +
    `👤 ${p.account_name || '_Not set_'}\n\n` +
    `🖼 Logo: ${p.logo_path ? '✅ Uploaded' : '❌ Not set'}\n` +
    `📄 Total Invoices: *${invs.length}*`,
    {
      parse_mode: 'Markdown',
      reply_markup: { inline_keyboard: [[
        { text: '✏️ Update Profile', callback_data: 'cmd_setup'     },
        { text: '📋 View Invoices',  callback_data: 'nav_invoices'  }
      ]]}
    }
  );
}

// ─── Invoice History ──────────────────────────────────────────────────────────
async function showInvoiceHistory(chatId, userId) {
  const invs = invoiceHistory[userId] || [];
  if (invs.length === 0) {
    await bot.sendMessage(chatId,
      '📋 *No invoices yet*\n\nCreate your first invoice by typing:\n_"Consulting for John Smith for 1500"_',
      { parse_mode: 'Markdown' }
    );
    return;
  }

  const recent   = invs.slice(-8).reverse();
  const currency = companyProfiles[userId]?.currency || 'AED';

  let msg = `📋 *Invoices* (${invs.length} total)\n━━━━━━━━━━━━━━━━━━━━━━\n\n`;

  recent.forEach((inv, i) => {
    const customer = inv.customer_name?.trim() || 'Unknown';
    const amount   = formatAmount(parseFloat(inv.total) || 0, inv.currency || currency);
    const dot      = i === 0 ? '🔵' : '⚪';
    msg += `${dot} \`${inv.invoice_id}\`\n`;
    msg += `   👤 ${customer}  💰 ${amount}\n`;
    msg += `   📅 ${inv.date}\n\n`;
  });

  await bot.sendMessage(chatId, msg, {
    parse_mode: 'Markdown',
    reply_markup: { inline_keyboard: [[
      { text: '📥 Download',  callback_data: 'nav_download' },
      { text: '📊 Stats',     callback_data: 'nav_stats'    }
    ]]}
  });
}

// ─── Customer List ────────────────────────────────────────────────────────────
function showCustomers(chatId, userId) {
  const invs = invoiceHistory[userId] || [];
  if (invs.length === 0) {
    bot.sendMessage(chatId, '👥 No customers yet.\n\nCreate an invoice to add your first customer!');
    return;
  }

  const customers = {};
  invs.forEach(inv => {
    const name = inv.customer_name?.trim();
    if (!name) return;
    if (!customers[name]) customers[name] = { count: 0, total: 0, currency: inv.currency, last: inv.date };
    customers[name].count++;
    customers[name].total += parseFloat(inv.total) || 0;
    customers[name].last   = inv.date;
  });

  const sorted = Object.entries(customers).sort((a, b) => b[1].total - a[1].total);

  let msg = `👥 *Customers* (${sorted.length} total)\n━━━━━━━━━━━━━━━━━━━━━━\n\n`;
  sorted.slice(0, 12).forEach(([name, d], i) => {
    msg += `${i + 1}. *${name}*\n`;
    msg += `   📄 ${d.count} invoice${d.count !== 1 ? 's' : ''}  💰 ${formatAmount(d.total, d.currency)}\n`;
    msg += `   📅 Last: ${d.last}\n\n`;
  });

  bot.sendMessage(chatId, msg, { parse_mode: 'Markdown' });
}

// ─── Period Selector ──────────────────────────────────────────────────────────
async function showPeriodSelector(chatId, userId, type) {
  commandState[userId] = { type };
  const prefix = type === 'stats' ? 'stats_' : 'dl_';
  const icon   = type === 'stats' ? '📊' : '📥';
  const title  = type === 'stats' ? 'View Statistics' : 'Download Invoices';

  await bot.sendMessage(chatId, `${icon} *${title}*\n\nSelect a time period:`, {
    parse_mode: 'Markdown',
    reply_markup: { inline_keyboard: [
      [
        { text: '📅 This Month',    callback_data: `${prefix}this_month`    },
        { text: '📅 Last Month',    callback_data: `${prefix}last_month`    }
      ],
      [
        { text: '📅 This Quarter',  callback_data: `${prefix}this_quarter`  },
        { text: '📅 This Year',     callback_data: `${prefix}this_year`     }
      ],
      [
        { text: '📅 All Time',      callback_data: `${prefix}all`           }
      ]
    ]}
  });
}

// ─── Filter Invoices ──────────────────────────────────────────────────────────
function filterInvoicesByPeriod(invoices, period) {
  const now = new Date();
  return invoices.filter(inv => {
    const parts = inv.date?.split('/');
    if (!parts || parts.length < 3) return false;
    const invDate = new Date(parseInt(parts[2]), parseInt(parts[1]) - 1, parseInt(parts[0]));
    if (isNaN(invDate)) return false;

    if (period === 'this_month') {
      return invDate.getMonth() === now.getMonth() && invDate.getFullYear() === now.getFullYear();
    }
    if (period === 'last_month') {
      const last = new Date(now.getFullYear(), now.getMonth() - 1, 1);
      return invDate.getMonth() === last.getMonth() && invDate.getFullYear() === last.getFullYear();
    }
    if (period === 'this_quarter') {
      const q = Math.floor(now.getMonth() / 3);
      return Math.floor(invDate.getMonth() / 3) === q && invDate.getFullYear() === now.getFullYear();
    }
    if (period === 'this_year') {
      return invDate.getFullYear() === now.getFullYear();
    }
    return true; // 'all'
  });
}

// ─── Stats ────────────────────────────────────────────────────────────────────
const PERIOD_NAMES = {
  this_month: 'This Month', last_month: 'Last Month',
  this_quarter: 'This Quarter', this_year: 'This Year', all: 'All Time'
};

async function showStats(chatId, userId, period) {
  const invs = invoiceHistory[userId] || [];
  if (invs.length === 0) {
    await bot.sendMessage(chatId, '📊 No invoices yet.\n\nCreate your first invoice to start tracking revenue!');
    return;
  }

  const filtered = filterInvoicesByPeriod(invs, period);
  if (filtered.length === 0) {
    await bot.sendMessage(chatId,
      `📊 No invoices found for *${PERIOD_NAMES[period] || period}*.`,
      { parse_mode: 'Markdown' }
    );
    return;
  }

  const currency = companyProfiles[userId]?.currency || 'AED';
  let total = 0, taxTotal = 0;
  filtered.forEach(inv => {
    total    += parseFloat(inv.total)      || 0;
    taxTotal += parseFloat(inv.tax_amount) || 0;
  });
  const subtotal = total - taxTotal;
  const avg      = total / filtered.length;

  // Top 3 customers
  const custTotals = {};
  filtered.forEach(inv => {
    const name = inv.customer_name?.trim();
    if (name) custTotals[name] = (custTotals[name] || 0) + (parseFloat(inv.total) || 0);
  });
  const topCustomers = Object.entries(custTotals).sort((a, b) => b[1] - a[1]).slice(0, 3);

  let msg = `📊 *Statistics — ${PERIOD_NAMES[period] || period}*\n━━━━━━━━━━━━━━━━━━━━━━\n\n`;
  msg += `📄 Invoices: *${filtered.length}*\n`;
  msg += `💰 Total Revenue: *${formatAmount(total, currency)}*\n`;
  msg += `📋 Subtotal (ex. tax): ${formatAmount(subtotal, currency)}\n`;
  if (taxTotal > 0) msg += `🏛 Tax Collected: ${formatAmount(taxTotal, currency)}\n`;
  msg += `📈 Average Invoice: *${formatAmount(avg, currency)}*\n`;

  if (topCustomers.length > 0) {
    const medals = ['🥇', '🥈', '🥉'];
    msg += `\n🏆 *Top Customers*\n`;
    topCustomers.forEach(([name, amt], i) => {
      msg += `${medals[i]} ${name} — ${formatAmount(amt, currency)}\n`;
    });
  }

  await bot.sendMessage(chatId, msg, {
    parse_mode: 'Markdown',
    reply_markup: { inline_keyboard: [[
      { text: '📥 Download These Invoices', callback_data: `dl_${period}` }
    ]]}
  });
}

// ─── Download / ZIP ───────────────────────────────────────────────────────────
function generateCSV(invoices) {
  const esc = (v) => {
    if (v === null || v === undefined) return '';
    const s = String(v).replace(/"/g, '""');
    return (s.includes(',') || s.includes('"') || s.includes('\n')) ? `"${s}"` : s;
  };
  let csv = 'Invoice ID,Date,Customer Name,Service,Subtotal,Tax Amount,Total,Currency,Payment Link\n';
  invoices.forEach(inv => {
    const sub = ((parseFloat(inv.total) || 0) - (parseFloat(inv.tax_amount) || 0)).toFixed(2);
    csv += [
      esc(inv.invoice_id), esc(inv.date), esc(inv.customer_name), esc(inv.service),
      esc(sub), esc(inv.tax_amount || '0.00'), esc(inv.total), esc(inv.currency),
      esc(inv.payment_link || '')
    ].join(',') + '\n';
  });
  return csv;
}

async function downloadInvoicesByPeriod(chatId, userId, period) {
  const invs     = invoiceHistory[userId] || [];
  const filtered = filterInvoicesByPeriod(invs, period);

  if (filtered.length === 0) {
    await bot.sendMessage(chatId,
      `📥 No invoices found for *${PERIOD_NAMES[period] || period}*.`,
      { parse_mode: 'Markdown' }
    );
    return;
  }

  try {
    await bot.sendMessage(chatId,
      `⏳ Preparing *${filtered.length}* invoice${filtered.length !== 1 ? 's' : ''} — *${PERIOD_NAMES[period] || period}*...`,
      { parse_mode: 'Markdown' }
    );

    const ts      = Date.now();
    const zipPath = `/tmp/invoices_${userId}_${ts}.zip`;
    const csvPath = `/tmp/invoices_${userId}_${ts}.csv`;
    const currency = companyProfiles[userId]?.currency || 'AED';

    fs.writeFileSync(csvPath, generateCSV(filtered));

    await new Promise((resolve, reject) => {
      const output  = fs.createWriteStream(zipPath);
      const archive = archiver('zip', { zlib: { level: 9 } });
      output.on('close', resolve);
      archive.on('error', reject);
      archive.pipe(output);
      archive.file(csvPath, { name: 'summary.csv' });
      filtered.forEach(inv => {
        if (inv.file_path && fs.existsSync(inv.file_path)) {
          archive.file(inv.file_path, { name: path.basename(inv.file_path) });
        }
      });
      archive.finalize();
    });

    const total = filtered.reduce((s, i) => s + (parseFloat(i.total) || 0), 0);

    await bot.sendDocument(chatId, zipPath, {
      caption:    `📦 *${PERIOD_NAMES[period]}* — ${filtered.length} invoice${filtered.length !== 1 ? 's' : ''}\n💰 Total: ${formatAmount(total, currency)}`,
      parse_mode: 'Markdown'
    });

    fs.unlinkSync(zipPath);
    fs.unlinkSync(csvPath);

  } catch (err) {
    console.error('Download error:', err);
    await bot.sendMessage(chatId, '⚠️ Error creating download. Please try again.');
  }
}

// ─── Delete Data Confirmation ─────────────────────────────────────────────────
async function confirmDeleteData(chatId, userId) {
  const count = (invoiceHistory[userId] || []).length;
  await bot.sendMessage(chatId,
    `🗑 *Delete All Your Data*\n\n` +
    `⚠️ This will permanently delete:\n` +
    `• Your business profile\n` +
    `• All *${count}* invoice record${count !== 1 ? 's' : ''}\n` +
    `• All customer data and statistics\n\n` +
    `This action *cannot be undone*.`,
    {
      parse_mode: 'Markdown',
      reply_markup: { inline_keyboard: [[
        { text: '🗑 Yes, Delete Everything', callback_data: 'deletedata_confirm' },
        { text: '❌ Cancel',                  callback_data: 'deletedata_cancel'  }
      ]]}
    }
  );
}

// ─── Voice Message ────────────────────────────────────────────────────────────
async function handleVoiceMessage(chatId, userId, voice, firstName) {
  try {
    await bot.sendMessage(chatId, '🎤 Processing your voice message...');

    const file      = await bot.getFile(voice.file_id);
    const fileUrl   = `https://api.telegram.org/file/bot${TELEGRAM_TOKEN}/${file.file_path}`;
    const res       = await axios.get(fileUrl, { responseType: 'arraybuffer', timeout: 30000 });
    const voicePath = `/tmp/voice_${userId}_${Date.now()}.ogg`;
    fs.writeFileSync(voicePath, Buffer.from(res.data));

    const transcription = await openai.audio.transcriptions.create({
      file:  fs.createReadStream(voicePath),
      model: 'whisper-1'
    });
    fs.unlinkSync(voicePath);

    const transcribedText = sanitizeInput(transcription.text);
    await bot.sendMessage(chatId,
      `🎤 *Heard:* _"${transcribedText}"_\n\n⚡ Processing...`,
      { parse_mode: 'Markdown' }
    );

    await handleTextMessage(chatId, userId, transcribedText, firstName);

  } catch (err) {
    console.error('Voice error:', err);
    await bot.sendMessage(chatId,
      '⚠️ Could not process your voice message. Please try again or type your invoice details.'
    );
  }
}

// ─── Text Message Routing ─────────────────────────────────────────────────────
async function handleTextMessage(chatId, userId, text, firstName) {
  const lower = text.toLowerCase();

  // Natural language download/stats commands
  if (lower.includes('download') || lower.includes('export')) {
    const period = lower.includes('this month') ? 'this_month'
                 : lower.includes('last month') ? 'last_month'
                 : lower.includes('quarter')    ? 'this_quarter'
                 : lower.includes('year')       ? 'this_year'
                 : null;
    if (period) { await downloadInvoicesByPeriod(chatId, userId, period); return; }
    await showPeriodSelector(chatId, userId, 'download');
    return;
  }

  if (lower.includes('stat') || lower.includes('revenue') || lower.includes('earning')) {
    const period = lower.includes('this month') ? 'this_month'
                 : lower.includes('last month') ? 'last_month'
                 : null;
    if (period) { await showStats(chatId, userId, period); return; }
    await showPeriodSelector(chatId, userId, 'stats');
    return;
  }

  // AI classification
  const intent = await classifyIntent(text);

  if (intent === 'invoice') {
    await processInvoiceRequest(chatId, userId, sanitizeInput(text));
  } else if (intent === 'greeting' || intent === 'help') {
    showWelcomeMessage(chatId, userId, firstName);
  } else {
    await bot.sendMessage(chatId,
      `❓ I didn't understand that.\n\n` +
      `To create an invoice, try:\n` +
      `_"Plumbing for Ahmed at Marina for 500"_\n\n` +
      `Or use /help for all available commands.`,
      { parse_mode: 'Markdown' }
    );
  }
}

// ─── Handle Command State (text-based period selection fallback) ───────────────
async function handleCommandState(chatId, userId, text) {
  const state = commandState[userId];
  if (!state) return;

  const lower  = text.toLowerCase();
  const period = lower.includes('this month')  ? 'this_month'
               : lower.includes('last month')  ? 'last_month'
               : lower.includes('quarter')     ? 'this_quarter'
               : lower.includes('year')        ? 'this_year'
               : lower.includes('all')         ? 'all'
               : null;

  if (period) {
    delete commandState[userId];
    if (state.type === 'stats') await showStats(chatId, userId, period);
    else await downloadInvoicesByPeriod(chatId, userId, period);
  } else {
    await bot.sendMessage(chatId, '⚠️ Please select a period using the buttons, or type "this month", "last month", "this year", or "all".');
  }
}

// ─── AI: Intent Classification ────────────────────────────────────────────────
async function classifyIntent(text) {
  try {
    const res = await axios.post('https://api.anthropic.com/v1/messages',
      {
        model:      'claude-haiku-4-5-20251001',
        max_tokens: 10,
        messages:   [{ role: 'user', content: `Classify as exactly one word — "invoice", "greeting", "help", or "invalid":\n"${text}"\nAnswer:` }]
      },
      { headers: { 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' } }
    );
    const intent = res.data.content[0].text.toLowerCase().trim().replace(/[^a-z]/g, '');
    return ['invoice', 'greeting', 'help', 'invalid'].includes(intent) ? intent : 'invalid';
  } catch (err) {
    console.error('Intent error:', err.message);
    return 'invalid';
  }
}

// ─── Invoice Data Validation ──────────────────────────────────────────────────
function validateExtractedData(data) {
  const errors = [];
  if (!data.customer_name?.trim())  errors.push('Missing customer name');
  if (!data.line_items?.length)     errors.push('Missing service / item description');
  const total = (data.line_items || []).reduce((s, i) => s + (parseFloat(i.amount) || 0), 0);
  if (total <= 0)                   errors.push('Amount must be greater than 0');
  return { valid: errors.length === 0, errors };
}

// ─── AI: Extract Invoice Data & Show Preview ──────────────────────────────────
async function processInvoiceRequest(chatId, userId, text) {
  try {
    await bot.sendMessage(chatId, '⚡ Reading your invoice details...');

    const res = await axios.post('https://api.anthropic.com/v1/messages',
      {
        model:      'claude-sonnet-4-5-20250929',
        max_tokens: 1024,
        messages:   [{
          role:    'user',
          content: `Extract invoice details from this text and return ONLY valid JSON, no other text.

Text: "${text}"

Return exactly this JSON structure:
{
  "customer_name": "full name of the person or company being billed",
  "address": "location or job address mentioned, or null",
  "line_items": [
    { "description": "service or item name", "amount": 0.00 }
  ]
}

Rules:
- customer_name: who is being billed
- line_items: one entry per service/item; if a single total covers multiple services, split equally
- amount: number only, no currency symbols
- address: delivery location or null`
        }]
      },
      { headers: { 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' } }
    );

    let raw = res.data.content[0].text.replace(/```json\n?|\n?```/g, '').trim();
    const m = raw.match(/\{[\s\S]*\}/);
    if (m) raw = m[0];

    let data;
    try {
      data = JSON.parse(raw);
    } catch {
      await bot.sendMessage(chatId,
        '⚠️ Could not parse invoice details. Please try:\n_"[Service] for [Customer] for [Amount]"_\n\nExample: _"Web design for John Smith for 2000"_',
        { parse_mode: 'Markdown' }
      );
      return;
    }

    const validation = validateExtractedData(data);
    if (!validation.valid) {
      await bot.sendMessage(chatId,
        `⚠️ *Missing information:*\n${validation.errors.map(e => `• ${e}`).join('\n')}\n\n` +
        `Please include: customer name, service description, and amount.\n\n` +
        `Example: _"Consulting for Ahmed Al-Rashidi for 1500"_`,
        { parse_mode: 'Markdown' }
      );
      return;
    }

    const profile  = companyProfiles[userId];
    const tc       = getTaxConfig(profile);
    const subtotal = data.line_items.reduce((s, i) => s + (parseFloat(i.amount) || 0), 0);
    const tax      = tc.enabled ? subtotal * (tc.rate / 100) : 0;
    const total    = subtotal + tax;

    // Store pending invoice
    pendingInvoices[userId] = { data, profile, subtotal, tax, total, tc };

    // Build preview message
    let preview = `📋 *Invoice Preview*\n━━━━━━━━━━━━━━━━━━━━━━\n\n`;
    preview += `👤 *Bill To:* ${data.customer_name}\n`;
    if (data.address && data.address !== 'null' && data.address !== null) {
      preview += `📍 ${data.address}\n`;
    }
    preview += `\n*Services:*\n`;
    data.line_items.forEach(item => {
      preview += `• ${item.description}: ${formatAmount(item.amount, profile.currency)}\n`;
    });
    preview += `\n━━━━━━━━━━━━━━━━━━━━━━\n`;
    preview += `Subtotal: ${formatAmount(subtotal, profile.currency)}\n`;
    if (tc.enabled) preview += `${tc.type} (${tc.rate}%): ${formatAmount(tax, profile.currency)}\n`;
    preview += `💰 *Total: ${formatAmount(total, profile.currency)}*`;

    await bot.sendMessage(chatId, preview, {
      parse_mode: 'Markdown',
      reply_markup: { inline_keyboard: [[
        { text: '✅ Generate Invoice', callback_data: 'confirm_invoice' },
        { text: '🔄 Try Again',        callback_data: 'retry_invoice'   }
      ]]}
    });

  } catch (err) {
    console.error('Invoice processing error:', err);
    await bot.sendMessage(chatId, '⚠️ Error processing your request. Please try again.');
  }
}

// ─── Confirm & Generate Invoice ───────────────────────────────────────────────
async function confirmAndGenerateInvoice(chatId, userId) {
  const pending = pendingInvoices[userId];
  if (!pending) {
    await bot.sendMessage(chatId, '⚠️ No pending invoice found. Please describe your invoice again.');
    return;
  }
  delete pendingInvoices[userId];

  try {
    await bot.sendMessage(chatId, '📄 Generating your PDF invoice...');

    const { data, profile, subtotal, tax, total, tc } = pending;
    const invoiceId = `INV-${Date.now()}`;
    const date      = new Date().toLocaleDateString('en-GB');

    const fullData = {
      customer_name:   data.customer_name,
      address:         data.address,
      company_name:    profile.company_name,
      company_address: profile.company_address,
      trn:             profile.trn,
      currency:        profile.currency,
      bank_name:       profile.bank_name,
      iban:            profile.iban,
      account_name:    profile.account_name,
      tax_enabled:     tc.enabled,
      tax_rate:        tc.rate,
      tax_type:        tc.type,
      logo_path:       profile.logo_path,
      invoice_id:      invoiceId,
      date:            date,
      line_items:      data.line_items,
      subtotal:        subtotal.toFixed(2),
      tax_amount:      tax.toFixed(2),
      total:           total.toFixed(2)
    };

    const pdfPath      = await generateProfessionalInvoice(fullData);
    const permanentPath = path.join(INVOICE_DIR, `${userId}_${invoiceId}.pdf`);
    fs.copyFileSync(pdfPath, permanentPath);

    if (!invoiceHistory[userId]) invoiceHistory[userId] = [];
    invoiceHistory[userId].push({
      invoice_id:    invoiceId,
      customer_name: data.customer_name,
      service:       data.line_items[0]?.description || 'Service',
      total:         total.toFixed(2),
      tax_amount:    tax.toFixed(2),
      currency:      profile.currency,
      date:          date,
      file_path:     permanentPath
    });
    saveData();

    // Payment link
    const paymentResult = await createPaymentLink({
      invoice_id:    invoiceId,
      customer_name: data.customer_name,
      total:         total.toFixed(2),
      currency:      profile.currency
    });

    let caption = `📄 *${invoiceId}*\n👤 ${data.customer_name}\n💰 *${formatAmount(total, profile.currency)}*`;
    if (paymentResult.success) {
      caption += `\n\n💳 *Pay Online:*\n${paymentResult.paymentUrl}`;
      invoiceHistory[userId][invoiceHistory[userId].length - 1].payment_link = paymentResult.paymentUrl;
      saveData();
    }

    await bot.sendDocument(chatId, pdfPath, { caption, parse_mode: 'Markdown' });
    fs.unlinkSync(pdfPath);

  } catch (err) {
    console.error('Invoice generation error:', err);
    await bot.sendMessage(chatId, '⚠️ Error generating invoice. Please try again.');
  }
}

// ─── PDF Generation ───────────────────────────────────────────────────────────
async function generateProfessionalInvoice(data) {
  return new Promise((resolve, reject) => {
    const pdfPath = `/tmp/invoice_${Date.now()}.pdf`;
    const doc     = new PDFDocument({ margin: 0, size: 'A4' });
    const stream  = fs.createWriteStream(pdfPath);
    doc.pipe(stream);

    const W         = 595.28;   // A4 width in points
    const MARGIN    = 40;
    const INNER_W   = W - MARGIN * 2;
    const NAVY      = '#1a1a2e';
    const ACCENT    = '#4361ee';
    const LIGHT_BG  = '#f4f6ff';
    const GREY_TEXT = '#666680';
    const BLACK     = '#1a1a2e';
    const WHITE     = '#ffffff';
    const curr      = CURRENCIES[data.currency] || { symbol: data.currency };

    const fmtAmt = (v) => {
      const n = parseFloat(v || 0).toFixed(2);
      return curr.right ? `${n} ${curr.symbol}` : `${curr.symbol}${n}`;
    };

    // ── Header bar ─────────────────────────────────────────────────────────────
    doc.rect(0, 0, W, 90).fill(NAVY);

    // Logo
    if (data.logo_path && fs.existsSync(data.logo_path)) {
      try { doc.image(data.logo_path, MARGIN, 12, { width: 60, height: 60 }); }
      catch (_) {}
    }

    // Company info
    const companyX = data.logo_path && fs.existsSync(data.logo_path) ? MARGIN + 72 : MARGIN;
    doc.fillColor(WHITE).font('Helvetica-Bold').fontSize(15)
       .text(data.company_name || '', companyX, 18, { width: 260 });
    doc.font('Helvetica').fontSize(8.5).fillColor('#b0b8e0')
       .text(data.company_address || '', companyX, 38, { width: 260 });
    if (data.trn) {
      doc.text(`TRN: ${data.trn}`, companyX, 54, { width: 260 });
    }

    // INVOICE label (right side of header)
    doc.fillColor(ACCENT).font('Helvetica-Bold').fontSize(24)
       .text('INVOICE', 390, 20, { align: 'right', width: 165 });
    doc.fillColor('#8899cc').font('Helvetica').fontSize(8)
       .text('For record-keeping purposes only', 390, 52, { align: 'right', width: 165 });

    // ── Meta row ───────────────────────────────────────────────────────────────
    let y = 100;
    doc.roundedRect(MARGIN, y, INNER_W, 44, 6).fill(LIGHT_BG);

    doc.font('Helvetica-Bold').fontSize(7.5).fillColor(GREY_TEXT)
       .text('INVOICE NUMBER', MARGIN + 14, y + 8);
    doc.font('Helvetica-Bold').fontSize(11).fillColor(BLACK)
       .text(data.invoice_id, MARGIN + 14, y + 20);

    doc.font('Helvetica-Bold').fontSize(7.5).fillColor(GREY_TEXT)
       .text('DATE', MARGIN + 200, y + 8);
    doc.font('Helvetica-Bold').fontSize(11).fillColor(BLACK)
       .text(data.date, MARGIN + 200, y + 20);

    // ── Bill To ────────────────────────────────────────────────────────────────
    y += 58;
    doc.roundedRect(MARGIN, y, 240, 62, 6).fill(LIGHT_BG);

    doc.font('Helvetica-Bold').fontSize(7.5).fillColor(GREY_TEXT)
       .text('BILL TO', MARGIN + 14, y + 10);
    doc.font('Helvetica-Bold').fontSize(12).fillColor(BLACK)
       .text(data.customer_name || '', MARGIN + 14, y + 22, { width: 212 });

    if (data.address && data.address !== 'null' && data.address !== null && data.address.trim() !== '') {
      doc.font('Helvetica').fontSize(9).fillColor(GREY_TEXT)
         .text(data.address, MARGIN + 14, y + 40, { width: 212 });
    }

    // ── Line Items Table ───────────────────────────────────────────────────────
    y += 78;

    // Table header
    doc.rect(MARGIN, y, INNER_W, 26).fill(NAVY);
    doc.font('Helvetica-Bold').fontSize(9).fillColor(WHITE)
       .text('DESCRIPTION', MARGIN + 12, y + 9)
       .text('AMOUNT', W - MARGIN - 65, y + 9, { align: 'right', width: 65 });
    y += 26;

    let rowNum = 0;
    (data.line_items || []).forEach(item => {
      // New page if needed
      if (y > 720) {
        doc.addPage({ margin: 0, size: 'A4' });
        y = 40;
      }
      const bg = rowNum % 2 === 0 ? WHITE : LIGHT_BG;
      doc.rect(MARGIN, y, INNER_W, 26).fill(bg);

      doc.font('Helvetica').fontSize(10).fillColor(BLACK)
         .text(item.description || '', MARGIN + 12, y + 8, { width: 380 });
      doc.text(fmtAmt(item.amount), W - MARGIN - 80, y + 8, { align: 'right', width: 80 });

      y += 26;
      rowNum++;
    });

    // ── Totals ─────────────────────────────────────────────────────────────────
    y += 8;
    if (y > 700) { doc.addPage({ margin: 0, size: 'A4' }); y = 40; }

    doc.moveTo(MARGIN, y).lineTo(W - MARGIN, y).strokeColor('#d8ddf0').lineWidth(1).stroke();
    y += 12;

    // Subtotal row
    doc.font('Helvetica').fontSize(10).fillColor(GREY_TEXT)
       .text('Subtotal', W - MARGIN - 200, y)
       .text(fmtAmt(data.subtotal), W - MARGIN - 80, y, { align: 'right', width: 80 });
    y += 20;

    // Tax row (if applicable)
    if (data.tax_enabled && parseFloat(data.tax_amount) > 0) {
      doc.text(`${data.tax_type} (${data.tax_rate}%)`, W - MARGIN - 200, y)
         .text(fmtAmt(data.tax_amount), W - MARGIN - 80, y, { align: 'right', width: 80 });
      y += 20;
    }

    // Total box
    if (y > 710) { doc.addPage({ margin: 0, size: 'A4' }); y = 40; }
    doc.rect(W - MARGIN - 210, y, 210, 34).fill(ACCENT);
    doc.font('Helvetica-Bold').fontSize(11).fillColor(WHITE)
       .text('TOTAL DUE', W - MARGIN - 198, y + 11)
       .text(fmtAmt(data.total), W - MARGIN - 80, y + 11, { align: 'right', width: 70 });
    y += 50;

    // ── Payment Details ────────────────────────────────────────────────────────
    if (y > 690) { doc.addPage({ margin: 0, size: 'A4' }); y = 40; }

    doc.roundedRect(MARGIN, y, INNER_W, 78, 6).fill(LIGHT_BG);
    doc.font('Helvetica-Bold').fontSize(7.5).fillColor(GREY_TEXT)
       .text('PAYMENT DETAILS', MARGIN + 14, y + 10);
    doc.font('Helvetica').fontSize(10).fillColor(BLACK)
       .text(`Bank: ${data.bank_name || 'N/A'}`,       MARGIN + 14, y + 24)
       .text(`IBAN: ${data.iban || 'N/A'}`,            MARGIN + 14, y + 40)
       .text(`Account: ${data.account_name || 'N/A'}`, MARGIN + 14, y + 56);

    // ── Footer ─────────────────────────────────────────────────────────────────
    doc.rect(0, 818, W, 23).fill(NAVY);
    doc.font('Helvetica').fontSize(7.5).fillColor('#8899cc')
       .text('Generated by InvoKash  ·  For record-keeping purposes only  ·  Not a legally certified tax document',
             MARGIN, 823, { align: 'center', width: INNER_W });

    doc.end();
    stream.on('finish', () => resolve(pdfPath));
    stream.on('error', reject);
  });
}

// ─── Lifecycle ────────────────────────────────────────────────────────────────
bot.on('polling_error', (err) => console.error('Polling error:', err.message));

process.on('SIGINT',  () => { saveData(); process.exit(0); });
process.on('SIGTERM', () => { saveData(); process.exit(0); });

setInterval(saveData, 5 * 60 * 1000);

console.log('✅ InvoKash Bot Ready!');
