/**
 * InvoKash — Telegram Bot (v2)
 * Enhanced UX: quick actions, invoice status, rich stats, voice support
 */

require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const axios       = require('axios');
const fs          = require('fs');
const path        = require('path');

const {
  companyProfiles, invoiceHistory, onboardingState, commandState, pendingInvoices,
  revenueGoals, expenseHistory,
  CURRENCIES, PERIOD_NAMES, LOGO_DIR, EXPENSE_CATEGORIES,
  checkRateLimit, sanitizeInput, formatAmount, getTaxConfig,
  filterInvoicesByPeriod, progressBar, asciiBar, calculateStats,
  classifyIntent, transcribeAudio, processInvoiceText, confirmInvoice,
  markInvoicePaid, buildDownloadZip, saveData,
  getLastInvoiceForCustomer, getAgingReport,
  setRevenueGoal, getRevenueGoal,
  generateBusinessInsights, generateClientStatement,
  saveTemplate, getTemplates, deleteTemplate,
  extractExpenseData, logExpense, getExpenses, calculateProfitLoss,
} = require('./core');

// ─── Bot Init ─────────────────────────────────────────────────────────────────
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;

// Bot is initialized lazily in startTelegramBot()
let bot;

// ─── Onboarding Config ────────────────────────────────────────────────────────
const ONBOARD_TOTAL = 10;

// ─── Currency paging state (for onboarding) ───────────────────────────────────
const currencyPage = {}; // { userId: 0|1 }

// ─── Helper: safe sendMessage ─────────────────────────────────────────────────
async function send(chatId, text, opts = {}) {
  try {
    return await bot.sendMessage(chatId, text, { parse_mode: 'Markdown', ...opts });
  } catch (err) {
    if (err.code !== 'ETELEGRAM') console.error('Send error:', err.message);
  }
}

// ─── Command Handler ──────────────────────────────────────────────────────────
async function handleCommand(chatId, userId, command, firstName) {
  const cmd = command.split(' ')[0].toLowerCase().split('@')[0];

  switch (cmd) {
    case '/start':     return showWelcome(chatId, userId, firstName);
    case '/setup':     return startOnboarding(chatId, userId, firstName);
    case '/help':      return showHelp(chatId);
    case '/profile':   return showProfile(chatId, userId);
    case '/invoices':  return showInvoices(chatId, userId);
    case '/customers': return showCustomers(chatId, userId);
    case '/stats':     return showPeriodSelector(chatId, userId, 'stats');
    case '/download':  return showPeriodSelector(chatId, userId, 'download');
    case '/deletedata':return confirmDeleteData(chatId, userId);
    case '/aging':     return showAgingDashboard(chatId, userId);
    case '/goal':      return showGoalSetter(chatId, userId);
    case '/statement': return selectClientForStatement(chatId, userId);
    case '/templates': return showTemplates(chatId, userId);
    case '/expenses':  return showExpenses(chatId, userId);
    case '/profit':    return showProfitLoss(chatId, userId, 'this_month');
    case '/agree':
      if (onboardingState[userId]) handleOnboarding(chatId, userId, 'agree');
      break;
    case '/cancel':
      if (onboardingState[userId])  { delete onboardingState[userId]; send(chatId, '❌ Setup cancelled.'); }
      else if (commandState[userId]){ delete commandState[userId];    send(chatId, '❌ Cancelled.'); }
      break;
    case '/skip':
      if (onboardingState[userId]) handleOnboarding(chatId, userId, 'skip');
      break;
    default:
      // Legacy text period commands
      if (['/this_month','/last_month','/this_quarter','/this_year','/all'].includes(cmd)) {
        const period = cmd.slice(1);
        const state  = commandState[userId];
        delete commandState[userId];
        if (state?.type === 'stats') showStats(chatId, userId, period);
        else downloadInvoices(chatId, userId, period);
      }
  }
}

// ─── Landing / Welcome ────────────────────────────────────────────────────────
async function showLanding(chatId, firstName) {
  await send(chatId,
    `✨ *Welcome to InvoKash* — Hi ${firstName}!\n\n` +
    `━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
    `Your AI invoice assistant that works by *voice or text* — no forms, no hassle.\n\n` +
    `*Just say it. We'll invoice it.*\n\n` +
    `🎤 _"Plumbing for Ahmed at Marina for 500"_\n` +
    `📄 → Professional PDF invoice, instantly\n` +
    `💳 → Stripe payment link, ready to share\n\n` +
    `━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
    `🌍 *14 currencies* · AED · USD · EUR · GBP · INR · SAR · OMR · more\n` +
    `🧾 VAT & GST compliant\n` +
    `📊 Revenue stats & CSV export\n` +
    `💬 Telegram + WhatsApp\n\n` +
    `⏱ Setup takes *~2 minutes*`,
    { reply_markup: { inline_keyboard: [
      [{ text: '🚀 Set Up My Business Account', callback_data: 'cmd_setup' }],
      [{ text: '❓ See How It Works',            callback_data: 'cmd_help'  }]
    ]}}
  );
}

function showWelcome(chatId, userId, firstName = 'there') {
  const profile = companyProfiles[userId];
  const history = invoiceHistory[userId] || [];

  if (!profile) return showLanding(chatId, firstName);

  const thisMonth  = filterInvoicesByPeriod(history, 'this_month');
  const lastMonth  = filterInvoicesByPeriod(history, 'last_month');
  const monthStats = calculateStats(thisMonth, profile.currency);
  const lastStats  = calculateStats(lastMonth, profile.currency);
  const allStats   = calculateStats(history, profile.currency);
  const curr       = CURRENCIES[profile.currency] || {};

  // Month-over-month trend
  const trend = lastStats.total > 0
    ? (((monthStats.total - lastStats.total) / lastStats.total) * 100).toFixed(0)
    : null;
  const trendIcon = trend === null ? '' : parseFloat(trend) >= 0 ? `📈 +${trend}%` : `📉 ${trend}%`;

  let msg = `🏠 *${profile.company_name}*\n`;
  msg += `${curr.flag || ''} ${profile.currency}`;
  if (profile.company_address) msg += `  ·  ${profile.company_address}`;
  msg += `\n\n`;

  msg += `━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;

  if (history.length === 0) {
    msg += `🌟 *Ready to invoice!*\n\n`;
    msg += `Create your first invoice right now:\n\n`;
  } else {
    msg += `📊 *This Month*\n`;
    msg += `📄 *${monthStats.count}* invoice${monthStats.count !== 1 ? 's' : ''}`;
    if (trendIcon) msg += `   ${trendIcon}`;
    msg += `\n`;
    msg += `💰 *${formatAmount(monthStats.total, profile.currency)}* revenue\n`;
    if (monthStats.paid > 0) msg += `✅ ${formatAmount(monthStats.paid, profile.currency)} collected\n`;
    if (monthStats.unpaid > 0) msg += `⏳ ${formatAmount(monthStats.unpaid, profile.currency)} outstanding\n`;
    msg += `\n`;
    msg += `📈 *All Time:* ${formatAmount(allStats.total, profile.currency)} across ${allStats.count} invoices\n\n`;
    msg += `━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;
  }

  // Revenue goal progress
  const goal = getRevenueGoal(userId);
  if (goal && goal.monthly > 0) {
    const pct = Math.min(100, Math.round((monthStats.total / goal.monthly) * 100));
    const bar = asciiBar(monthStats.total, goal.monthly, 12);
    msg += `🎯 *Monthly Goal*\n`;
    msg += `${bar} ${pct}%\n`;
    msg += `${formatAmount(monthStats.total, profile.currency)} of ${formatAmount(goal.monthly, profile.currency)}\n\n`;
    msg += `━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;
  }

  msg += `🎤 *Create an Invoice*\n`;
  msg += `_Just type or send a voice message:_\n\n`;
  msg += `_"Website design for Acme Corp for 3500"_\n`;
  msg += `_"Plumbing at Marina for Ahmed for 500"_`;

  send(chatId, msg, {
    reply_markup: { inline_keyboard: [
      [
        { text: '📊 Stats',      callback_data: 'nav_stats'     },
        { text: '📋 Invoices',   callback_data: 'nav_invoices'  },
      ],
      [
        { text: '👥 Customers',  callback_data: 'nav_customers' },
        { text: '📥 Download',   callback_data: 'nav_download'  },
      ],
      [
        { text: '⏱ Aging',      callback_data: 'nav_aging'     },
        { text: '📈 P&L',       callback_data: 'nav_profit'    },
      ],
      [
        { text: '👤 Profile',    callback_data: 'nav_profile'   },
        { text: '🎯 Goal',       callback_data: 'nav_goal'      },
      ]
    ]}
  });
}

// ─── Help ─────────────────────────────────────────────────────────────────────
function showHelp(chatId) {
  send(chatId,
    `📖 *InvoKash — Help Guide*\n` +
    `━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
    `*🎤 Creating Invoices*\n` +
    `Just type or send a voice note — no forms!\n\n` +
    `_"Plumbing for Ahmed at Marina for 500"_\n` +
    `_"Web design for Acme Corp for 3000"_\n` +
    `_"Design, hosting & support for TechCo for 2500"_\n\n` +
    `━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
    `*📋 Commands*\n` +
    `/start — 🏠 Home dashboard\n` +
    `/setup — ⚙️ Set up or update profile\n` +
    `/profile — 👤 View business profile\n` +
    `/invoices — 📋 Recent invoices + mark paid\n` +
    `/customers — 👥 Customer directory\n` +
    `/stats — 📊 Revenue analytics\n` +
    `/download — 📥 Export PDF + CSV bundle\n` +
    `/aging — ⏱ Invoice aging (30/60/90 days)\n` +
    `/goal — 🎯 Set monthly revenue goal\n` +
    `/statement — 📄 Client statement PDF\n` +
    `/templates — 📌 Invoice templates\n` +
    `/expenses — 💸 Track expenses\n` +
    `/profit — 📈 Profit & loss report\n` +
    `/deletedata — 🗑 Delete all your data\n\n` +
    `━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
    `*💡 Pro Tips*\n` +
    `• Include: _customer name + service + amount_\n` +
    `• Multi-item: _"Design and hosting for Client for 2000"_\n` +
    `• Tap ✅ *Mark as Paid* when a client settles up\n` +
    `• Voice messages work in any language!\n\n` +
    `*🌍 Currencies*\n` +
    `AED · USD · EUR · GBP · INR · SAR\n` +
    `OMR · KWD · BHD · QAR · EGP · SGD · CAD · AUD\n\n` +
    `*💬 Support:* @${process.env.SUPPORT_USERNAME || 'InvoKashSupport'}`,
    { reply_markup: { inline_keyboard: [[
      { text: '🏠 Home', callback_data: 'nav_home' },
      { text: '⚙️ Setup', callback_data: 'cmd_setup' }
    ]]}}
  );
}

// ─── Onboarding ───────────────────────────────────────────────────────────────
function startOnboarding(chatId, userId, firstName = 'there') {
  const isUpdate = !!companyProfiles[userId];
  onboardingState[userId] = { step: 'disclaimer' };

  send(chatId,
    (isUpdate
      ? `⚙️ *Update Your Profile*\n\nThis will replace your current settings.\n\n`
      : `🎉 *Let\'s set up your account, ${firstName}!*\n\n`) +
    `⚠️ *Disclaimer*\n\n` +
    `InvoKash generates invoices for *record-keeping purposes only*. These are not legally certified tax documents.\n\n` +
    `By proceeding you confirm:\n` +
    `• You are responsible for tax compliance in your jurisdiction\n` +
    `• Your data is stored securely and never shared with third parties\n` +
    `• You can delete all data at any time with /deletedata`,
    { reply_markup: { inline_keyboard: [[
      { text: '✅ I Agree — Continue', callback_data: 'setup_agree'  },
      { text: '❌ Cancel',             callback_data: 'setup_cancel' }
    ]]}}
  );
}

async function handleLogoUpload(chatId, userId, photos) {
  try {
    const photo    = photos[photos.length - 1];
    const file     = await bot.getFile(photo.file_id);
    const fileUrl  = `https://api.telegram.org/file/bot${TELEGRAM_TOKEN}/${file.file_path}`;
    const res      = await axios.get(fileUrl, { responseType: 'arraybuffer', timeout: 30000 });
    const logoPath = path.join(LOGO_DIR, `logo_${userId}.jpg`);
    fs.writeFileSync(logoPath, Buffer.from(res.data));
    companyProfiles[userId].logo_path = logoPath;
    saveData();
  } catch (err) {
    console.error('Logo upload error:', err.message);
    companyProfiles[userId].logo_path = null;
  }
  delete onboardingState[userId];
  await sendSetupComplete(chatId, userId);
}

function showCurrencyPage(chatId, userId, page) {
  const allCurrencies = Object.entries(CURRENCIES);
  const perPage = 6;
  const start = page * perPage;
  const slice = allCurrencies.slice(start, start + perPage);

  const rows = [];
  for (let i = 0; i < slice.length; i += 3) {
    rows.push(slice.slice(i, i + 3).map(([code, cfg]) => ({
      text: `${cfg.flag} ${code}`, callback_data: `currency_${code}`
    })));
  }

  const nav = [];
  if (page > 0) nav.push({ text: '◀ Back', callback_data: 'currency_back' });
  if (start + perPage < allCurrencies.length) nav.push({ text: 'More ▶', callback_data: 'currency_more' });
  if (nav.length > 0) rows.push(nav);

  send(chatId,
    `${progressBar(4, ONBOARD_TOTAL)}\n\n` +
    `💰 *Invoice Currency*\n\nSelect the currency you invoice in:`,
    { reply_markup: { inline_keyboard: rows } }
  );
}

async function handleOnboarding(chatId, userId, text) {
  const state = onboardingState[userId];
  if (!state) return;

  if (!companyProfiles[userId]) companyProfiles[userId] = {};
  const input = (text || '').toLowerCase().trim();
  const p     = companyProfiles[userId];

  switch (state.step) {

    case 'disclaimer':
      if (input !== 'agree') return;
      state.step = 'company_name';
      send(chatId,
        `${progressBar(1, ONBOARD_TOTAL)}\n\n` +
        `🏢 *Step 1 — Company Name*\n\nWhat is your business or trading name?`
      );
      break;

    case 'company_name':
      if (!text?.trim()) return send(chatId, '⚠️ Please enter a valid company name.');
      p.company_name = sanitizeInput(text);
      state.step = 'company_address';
      send(chatId,
        `${progressBar(2, ONBOARD_TOTAL)}\n\n` +
        `📍 *Step 2 — Business Address*\n\nEnter your full business address:`,
        { reply_markup: { inline_keyboard: [[{ text: '⏭ Skip', callback_data: 'setup_skip' }]] }}
      );
      break;

    case 'company_address':
      p.company_address = input === 'skip' ? '' : sanitizeInput(text);
      state.step = 'trn';
      send(chatId,
        `${progressBar(3, ONBOARD_TOTAL)}\n\n` +
        `🔐 *Step 3 — Tax Registration Number*\n\nEnter your TRN / VAT / GST registration number (optional):`,
        { reply_markup: { inline_keyboard: [[{ text: '⏭ Skip — No TRN', callback_data: 'setup_skip' }]] }}
      );
      break;

    case 'trn':
      p.trn = input === 'skip' ? '' : sanitizeInput(text);
      state.step = 'currency';
      showCurrencyPage(chatId, userId, 0);
      break;

    case 'currency': {
      const curr = (text || '').toUpperCase().trim();
      if (!CURRENCIES[curr]) return send(chatId, '⚠️ Please select a currency using the buttons above.');
      p.currency = curr;
      state.step = 'bank_name';
      send(chatId,
        `${progressBar(5, ONBOARD_TOTAL)}\n\n` +
        `🏦 *Step 5 — Bank Name*\n\nEnter your bank name:\n_e.g. Emirates NBD, HDFC, Barclays, Chase_`
      );
      break;
    }

    case 'bank_name':
      p.bank_name = sanitizeInput(text);
      state.step = 'iban';
      send(chatId,
        `${progressBar(6, ONBOARD_TOTAL)}\n\n` +
        `🔑 *Step 6 — ${p.currency === 'INR' ? 'Account Number & IFSC' : 'IBAN'}*\n\n` +
        `Enter your ${p.currency === 'INR' ? 'account number and IFSC code' : 'IBAN'}:`
      );
      break;

    case 'iban':
      p.iban = sanitizeInput(text);
      state.step = 'account_name';
      send(chatId,
        `${progressBar(7, ONBOARD_TOTAL)}\n\n` +
        `👤 *Step 7 — Account Holder Name*\n\nName on the bank account:`
      );
      break;

    case 'account_name':
      p.account_name = sanitizeInput(text);
      state.step = 'tax_enabled';
      const taxType = CURRENCIES[p.currency]?.tax || 'VAT';
      send(chatId,
        `${progressBar(8, ONBOARD_TOTAL)}\n\n` +
        `📊 *Step 8 — Tax Settings*\n\nDo you charge *${taxType}* on your invoices?`,
        { reply_markup: { inline_keyboard: [[
          { text: `✅ Yes, I charge ${taxType}`, callback_data: 'tax_yes' },
          { text: '❌ No tax',                   callback_data: 'tax_no'  }
        ]]}}
      );
      break;

    case 'tax_enabled': {
      const taxField = ['INR','SGD','AUD'].includes(p.currency) ? 'gst' : 'vat';
      if (input === 'yes') {
        p[`${taxField}_enabled`] = true;
        if (taxField === 'gst') { p.vat_enabled = false; p.vat_rate = 0; }
        else                    { p.gst_enabled = false; p.gst_rate = 0; }
        state.step = 'tax_rate';
        send(chatId,
          `${progressBar(9, ONBOARD_TOTAL)}\n\n` +
          `📈 *Step 9 — ${taxField.toUpperCase()} Rate*\n\nEnter the percentage (e.g. \`5\` for 5%):`,
        );
      } else if (input === 'no') {
        p.vat_enabled = false; p.vat_rate = 0;
        p.gst_enabled = false; p.gst_rate = 0;
        state.step = 'logo';
        sendLogoPrompt(chatId);
      } else {
        send(chatId, '⚠️ Please tap Yes or No using the buttons above.');
      }
      break;
    }

    case 'tax_rate': {
      const rate = parseFloat(text);
      if (isNaN(rate) || rate < 0 || rate > 100) return send(chatId, '⚠️ Enter a number between 0 and 100 (e.g., 5).');
      const taxField = ['INR','SGD','AUD'].includes(p.currency) ? 'gst' : 'vat';
      p[`${taxField}_rate`] = rate;
      state.step = 'logo';
      sendLogoPrompt(chatId);
      break;
    }

    case 'logo':
      if (input === 'skip') {
        p.logo_path = null;
        delete onboardingState[userId];
        saveData();
        await sendSetupComplete(chatId, userId);
      }
      break;
  }
}

function sendLogoPrompt(chatId) {
  send(chatId,
    `${progressBar(10, ONBOARD_TOTAL)}\n\n` +
    `🖼 *Step 10 — Company Logo (Optional)*\n\nSend your logo as a PNG or JPG, or skip to use text header.`,
    { reply_markup: { inline_keyboard: [[{ text: '⏭ Skip Logo', callback_data: 'setup_skip' }]] }}
  );
}

async function sendSetupComplete(chatId, userId) {
  const p    = companyProfiles[userId];
  const tc   = getTaxConfig(p);
  const curr = CURRENCIES[p.currency] || {};

  await send(chatId,
    `🎉 *Setup Complete!*\n` +
    `━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
    `*${p.company_name}* is ready to invoice!\n\n` +
    `🏢 ${p.company_name}\n` +
    `📍 ${p.company_address || '_No address set_'}\n` +
    `${curr.flag || ''} ${p.currency} — ${curr.name || ''}\n` +
    `📊 ${tc.type}: ${tc.enabled ? `*${tc.rate}%*` : 'Not charged'}\n` +
    `🏦 ${p.bank_name || '_No bank set_'}\n` +
    `🖼 Logo: ${p.logo_path ? '✅ Uploaded' : '⬜ Text header'}\n\n` +
    `━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
    `*You\'re all set!* 🚀\n\n` +
    `Create your first invoice now — just type:\n` +
    `_"Web design for Acme Corp for 3000"_\n\n` +
    `Or send a 🎤 voice message!`,
    { reply_markup: { inline_keyboard: [
      [{ text: '🏠 Go to Dashboard', callback_data: 'nav_home' }],
      [{ text: '📖 How to Create Invoices', callback_data: 'cmd_help' }]
    ]}}
  );
}

// ─── Profile ──────────────────────────────────────────────────────────────────
function showProfile(chatId, userId) {
  const p = companyProfiles[userId];
  if (!p) return send(chatId,
    `👤 *No Profile Found*\n\nUse /setup to create your business profile.`,
    { reply_markup: { inline_keyboard: [[{ text: '🚀 Set Up Now', callback_data: 'cmd_setup' }]] }});

  const curr  = CURRENCIES[p.currency] || {};
  const tc    = getTaxConfig(p);
  const invs  = invoiceHistory[userId] || [];
  const stats = calculateStats(invs, p.currency);

  // Paid collection rate
  const paidPct = stats.total > 0 ? Math.round((stats.paid / stats.total) * 100) : 0;
  const collBar = asciiBar(stats.paid, stats.total || 1, 10);

  send(chatId,
    `👤 *Business Profile*\n` +
    `━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
    `🏢 *${p.company_name}*\n` +
    `📍 ${p.company_address || '_Not set_'}\n` +
    `${p.trn ? `🔐 TRN: \`${p.trn}\`` : '🔐 _No TRN / VAT number_'}\n\n` +
    `💰 *Invoice Currency*\n` +
    `${curr.flag || '🌍'} *${p.currency}* — ${curr.name || ''}\n` +
    `📊 ${tc.type}: ${tc.enabled ? `✅ *${tc.rate}%*` : '❌ Not charged'}\n\n` +
    `🏦 *Bank Details*\n` +
    `🏛 ${p.bank_name    || '_Not set_'}\n` +
    `🔑 ${p.iban         || '_Not set_'}\n` +
    `👤 ${p.account_name || '_Not set_'}\n` +
    `🖼 Logo: ${p.logo_path ? '✅ Uploaded' : '⬜ Text header'}\n\n` +
    `━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
    `📈 *Lifetime Performance*\n` +
    `📄 ${stats.count} invoices  ·  ${formatAmount(stats.total, p.currency)}\n` +
    `✅ Paid: ${formatAmount(stats.paid, p.currency)}\n` +
    `⏳ Outstanding: ${formatAmount(stats.unpaid, p.currency)}\n` +
    `${collBar} ${paidPct}% collected`,
    { reply_markup: { inline_keyboard: [
      [
        { text: '✏️ Update Profile', callback_data: 'cmd_setup'    },
        { text: '📊 My Stats',       callback_data: 'nav_stats'    },
      ],
      [{ text: '🏠 Home', callback_data: 'nav_home' }]
    ]}}
  );
}

// ─── Invoice History ──────────────────────────────────────────────────────────
async function showInvoices(chatId, userId) {
  const invs = invoiceHistory[userId] || [];
  if (invs.length === 0) {
    return send(chatId,
      `📋 *No Invoices Yet*\n\n` +
      `━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
      `Create your first invoice now!\n\n` +
      `Just type:\n_"Consulting for John Smith for 1500"_\n\n` +
      `Or send a 🎤 voice message`
    );
  }

  const currency = companyProfiles[userId]?.currency || 'AED';
  const recent   = invs.slice(-10).reverse();
  const unpaidCt = invs.filter(i => i.status !== 'paid').length;

  let msg = `📋 *Recent Invoices*\n`;
  msg += `━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;
  if (unpaidCt > 0) msg += `⏳ *${unpaidCt} unpaid* invoice${unpaidCt !== 1 ? 's' : ''}\n\n`;

  recent.forEach((inv, i) => {
    const customer = inv.customer_name?.trim() || 'Unknown';
    const amount   = formatAmount(parseFloat(inv.total) || 0, inv.currency || currency);
    const status   = inv.status === 'paid' ? '✅' : '⏳';
    const recency  = i === 0 ? ' ← *latest*' : '';
    msg += `${status} \`${inv.invoice_id}\`${recency}\n`;
    msg += `👤 ${customer}  💰 *${amount}*\n`;
    msg += `📅 ${inv.date}\n\n`;
  });

  if (invs.length > 10) msg += `_+${invs.length - 10} older invoices in download_\n`;

  // Mark-paid buttons for most recent unpaid
  const unpaid = recent.filter(i => i.status !== 'paid').slice(0, 2);
  const keyboard = [];
  if (unpaid.length > 0) {
    keyboard.push(unpaid.map(inv => ({
      text: `✅ Mark Paid — ${inv.invoice_id}`,
      callback_data: `paid_${inv.invoice_id}`
    })));
  }
  keyboard.push([
    { text: '📥 Download All',  callback_data: 'nav_download' },
    { text: '📊 Stats',         callback_data: 'nav_stats'    }
  ]);
  keyboard.push([{ text: '🏠 Home', callback_data: 'nav_home' }]);

  await send(chatId, msg, { reply_markup: { inline_keyboard: keyboard } });
}

// ─── Mark Invoice Paid ────────────────────────────────────────────────────────
function handleMarkPaid(chatId, userId, invoiceId) {
  const result = markInvoicePaid(userId, invoiceId);
  if (result) {
    // Find the invoice for amount
    const inv = (invoiceHistory[userId] || []).find(i => i.invoice_id === invoiceId);
    const amtStr = inv ? ` · ${formatAmount(inv.total, inv.currency)}` : '';
    send(chatId,
      `✅ *Payment Received!*\n\n` +
      `\`${invoiceId}\`${amtStr}\n\n` +
      `💰 Cash in the bank! Great work. 🎉`,
      { reply_markup: { inline_keyboard: [[
        { text: '📊 View Stats',    callback_data: 'nav_stats'    },
        { text: '📋 All Invoices',  callback_data: 'nav_invoices' }
      ]]}}
    );
  } else {
    send(chatId, `⚠️ Invoice \`${invoiceId}\` not found.`);
  }
}

// ─── Customers ────────────────────────────────────────────────────────────────
function showCustomers(chatId, userId) {
  const invs = invoiceHistory[userId] || [];
  if (invs.length === 0) return send(chatId,
    `👥 *No Customers Yet*\n\nCreate your first invoice to build your client directory!\n\n_"Consulting for John Smith for 1500"_`);

  const customers = {};
  invs.forEach(inv => {
    const name = inv.customer_name?.trim();
    if (!name) return;
    if (!customers[name]) customers[name] = { count: 0, total: 0, paid: 0, currency: inv.currency, last: inv.date };
    customers[name].count++;
    customers[name].total += parseFloat(inv.total) || 0;
    if (inv.status === 'paid') customers[name].paid += parseFloat(inv.total) || 0;
    customers[name].last = inv.date;
  });

  const sorted     = Object.entries(customers).sort((a, b) => b[1].total - a[1].total);
  const maxRevenue = sorted[0]?.[1].total || 1;
  const currency   = companyProfiles[userId]?.currency || 'AED';

  let msg = `👥 *Client Directory*\n`;
  msg += `━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;
  msg += `${sorted.length} client${sorted.length !== 1 ? 's' : ''} · `;
  const totalRevenue = sorted.reduce((s, [,d]) => s + d.total, 0);
  msg += `${formatAmount(totalRevenue, currency)} lifetime\n\n`;

  const medals = ['🥇', '🥈', '🥉'];
  sorted.slice(0, 10).forEach(([name, d], i) => {
    const bar     = asciiBar(d.total, maxRevenue, 10);
    const badge   = i < 3 ? medals[i] : `${i + 1}.`;
    const paidPct = d.total > 0 ? Math.round((d.paid / d.total) * 100) : 0;
    msg += `${badge} *${name}*\n`;
    msg += `   ${bar}  ${formatAmount(d.total, d.currency)}\n`;
    msg += `   📄 ${d.count} invoice${d.count !== 1 ? 's' : ''}`;
    if (d.paid > 0) msg += `  ·  ✅ ${paidPct}% paid`;
    msg += `  ·  Last: ${d.last}\n\n`;
  });

  if (sorted.length > 10) msg += `_+${sorted.length - 10} more clients in download_\n`;

  send(chatId, msg, { reply_markup: { inline_keyboard: [
    [
      { text: '📊 Revenue Stats', callback_data: 'nav_stats'    },
      { text: '📥 Export CSV',    callback_data: 'nav_download' },
    ],
    [{ text: '🏠 Home', callback_data: 'nav_home' }]
  ]}});
}

// ─── Period Selector ──────────────────────────────────────────────────────────
async function showPeriodSelector(chatId, userId, type) {
  commandState[userId] = { type };
  const prefix = type === 'stats' ? 'stats_' : 'dl_';
  const icon   = type === 'stats' ? '📊' : '📥';

  await send(chatId, `${icon} *${type === 'stats' ? 'Revenue Statistics' : 'Download Invoices'}*\n\nSelect a time period:`, {
    reply_markup: { inline_keyboard: [
      [
        { text: '📅 This Month',   callback_data: `${prefix}this_month`   },
        { text: '📅 Last Month',   callback_data: `${prefix}last_month`   }
      ],
      [
        { text: '📅 This Quarter', callback_data: `${prefix}this_quarter` },
        { text: '📅 This Year',    callback_data: `${prefix}this_year`    }
      ],
      [{ text: '📅 All Time',      callback_data: `${prefix}all`          }]
    ]}
  });
}

// ─── Stats ────────────────────────────────────────────────────────────────────
async function showStats(chatId, userId, period) {
  const invs = invoiceHistory[userId] || [];
  if (invs.length === 0) return send(chatId,
    `📊 *No Invoices Yet*\n\nCreate your first invoice to start tracking revenue!\n\n_"Consulting for Client for 1500"_`);

  const filtered = filterInvoicesByPeriod(invs, period);
  if (filtered.length === 0) return send(chatId,
    `📊 No invoices found for *${PERIOD_NAMES[period] || period}*.\n\nTry a different period:`,
    { reply_markup: { inline_keyboard: [[
      { text: '📅 All Time', callback_data: 'stats_all' },
      { text: '📅 This Year', callback_data: 'stats_this_year' }
    ]]}}
  );

  const currency = companyProfiles[userId]?.currency || 'AED';
  const stats    = calculateStats(filtered, currency);

  // Paid collection progress bar
  const paidPct  = stats.total > 0 ? Math.round((stats.paid / stats.total) * 100) : 0;
  const paidBar  = asciiBar(stats.paid, stats.total || 1, 12);

  // 6-month trend (last 6 months from all invoices for context)
  const now = new Date();
  const monthlyData = [];
  for (let i = 5; i >= 0; i--) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
    const monthInvs = invs.filter(inv => {
      const parts = inv.date?.split('/');
      if (!parts || parts.length < 3) return false;
      const id = new Date(parseInt(parts[2]), parseInt(parts[1]) - 1, 1);
      return id.getMonth() === d.getMonth() && id.getFullYear() === d.getFullYear();
    });
    const total = monthInvs.reduce((s, inv) => s + (parseFloat(inv.total) || 0), 0);
    monthlyData.push({ month: d.toLocaleString('en-US', { month: 'short' }), total });
  }
  const maxMonth = Math.max(...monthlyData.map(m => m.total), 1);

  let msg = `📊 *${PERIOD_NAMES[period] || period} — Revenue Report*\n`;
  msg += `━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;

  msg += `📄 *${stats.count}* invoice${stats.count !== 1 ? 's' : ''}\n`;
  msg += `💰 *Revenue:*  ${formatAmount(stats.total, currency)}\n`;
  if (stats.taxTotal > 0) {
    msg += `📋 Subtotal:  ${formatAmount(stats.subtotal, currency)}\n`;
    msg += `🏛 Tax:  ${formatAmount(stats.taxTotal, currency)}\n`;
  }
  msg += `📈 Average:  ${formatAmount(stats.avg, currency)}\n\n`;

  msg += `*💳 Collection*\n`;
  msg += `${paidBar} ${paidPct}%\n`;
  msg += `✅ Collected:  ${formatAmount(stats.paid, currency)}\n`;
  msg += `⏳ Outstanding:  ${formatAmount(stats.unpaid, currency)}\n`;

  if (stats.topCustomers.length > 0) {
    const maxCust = stats.topCustomers[0][1];
    const medals  = ['🥇', '🥈', '🥉', '4️⃣', '5️⃣'];
    msg += `\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;
    msg += `🏆 *Top Customers*\n\n`;
    stats.topCustomers.forEach(([name, amt], i) => {
      const bar = asciiBar(amt, maxCust, 10);
      msg += `${medals[i]} *${name}*\n`;
      msg += `   ${bar}  ${formatAmount(amt, currency)}\n\n`;
    });
  }

  // 6-month chart (always show for visual appeal)
  if (monthlyData.some(m => m.total > 0)) {
    msg += `━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;
    msg += `📅 *Last 6 Months*\n\n`;
    monthlyData.forEach(({ month, total }) => {
      const bar = asciiBar(total, maxMonth, 10);
      const amt = total > 0 ? `  ${formatAmount(total, currency)}` : '  —';
      msg += `\`${month}\` ${bar}${amt}\n`;
    });
  }

  await send(chatId, msg, { reply_markup: { inline_keyboard: [
    [
      { text: '🤖 AI Insights',       callback_data: `insights_${period}` },
      { text: '⏱ Aging Report',       callback_data: 'nav_aging'          },
    ],
    [
      { text: '📥 Download Invoices', callback_data: `dl_${period}`       },
      { text: '👥 Customers',         callback_data: 'nav_customers'      },
    ],
    [{ text: '🏠 Home', callback_data: 'nav_home' }]
  ]}});
}

// ─── Download ─────────────────────────────────────────────────────────────────
async function downloadInvoices(chatId, userId, period) {
  const invs = invoiceHistory[userId] || [];
  const filtered = filterInvoicesByPeriod(invs, period);
  if (filtered.length === 0) return send(chatId, `📥 No invoices for *${PERIOD_NAMES[period] || period}*.`);

  await send(chatId, `⏳ Preparing *${filtered.length}* invoice${filtered.length !== 1 ? 's' : ''}...`);

  try {
    const result   = await buildDownloadZip(userId, period);
    if (!result) return send(chatId, '⚠️ Error building download. Please try again.');

    const { zipPath, stats, currency } = result;
    const caption =
      `📦 *${PERIOD_NAMES[period]}*\n` +
      `📄 ${stats.count} invoice${stats.count !== 1 ? 's' : ''}\n` +
      `💰 ${formatAmount(stats.total, currency)}\n` +
      `✅ Paid: ${formatAmount(stats.paid, currency)}`;

    await bot.sendDocument(chatId, zipPath, { caption, parse_mode: 'Markdown' });
    try { fs.unlinkSync(zipPath); } catch (_) {}
  } catch (err) {
    console.error('Download error:', err.message);
    send(chatId, '⚠️ Error creating download. Please try again.');
  }
}

// ─── Delete Data ──────────────────────────────────────────────────────────────
async function confirmDeleteData(chatId, userId) {
  const count = (invoiceHistory[userId] || []).length;
  const invs  = invoiceHistory[userId] || [];
  const stats = calculateStats(invs, companyProfiles[userId]?.currency || 'AED');
  await send(chatId,
    `🗑 *Delete All Data*\n` +
    `━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
    `⚠️ *This will permanently erase:*\n\n` +
    `• Business profile & settings\n` +
    `• *${count}* invoice record${count !== 1 ? 's' : ''}\n` +
    `• ${formatAmount(stats.total, companyProfiles[userId]?.currency || 'AED')} revenue history\n` +
    `• All customer & statistics data\n\n` +
    `*This action cannot be undone.*\n\n` +
    `_Download your invoices first? Use /download_`,
    { reply_markup: { inline_keyboard: [
      [{ text: '🗑 Yes, Delete Everything', callback_data: 'deletedata_confirm' }],
      [{ text: '❌ Cancel — Keep My Data',  callback_data: 'deletedata_cancel'  }],
    ]}}
  );
}

// ─── Voice Message ────────────────────────────────────────────────────────────
async function handleVoiceMessage(chatId, userId, voice, firstName) {
  try {
    await send(chatId, '🎤 _Listening... transcribing your voice note_');

    const file      = await bot.getFile(voice.file_id);
    const fileUrl   = `https://api.telegram.org/file/bot${TELEGRAM_TOKEN}/${file.file_path}`;
    const res       = await axios.get(fileUrl, { responseType: 'arraybuffer', timeout: 30000 });
    const voicePath = `/tmp/voice/${userId}_${Date.now()}.ogg`;
    fs.writeFileSync(voicePath, Buffer.from(res.data));

    let transcribedText;
    try {
      transcribedText = await transcribeAudio(voicePath);
    } finally {
      try { fs.unlinkSync(voicePath); } catch (_) {}
    }

    transcribedText = sanitizeInput(transcribedText);
    await send(chatId,
      `🎤 *Heard you say:*\n_"${transcribedText}"_\n\n` +
      `⚡ Extracting invoice details...`
    );
    await handleTextMessage(chatId, userId, transcribedText, firstName);

  } catch (err) {
    console.error('Voice error:', err.message);
    send(chatId,
      `⚠️ *Voice note couldn't be processed*\n\n` +
      `Please type your invoice instead:\n` +
      `_"Plumbing for Ahmed at Marina for 500"_`
    );
  }
}

// ─── Text Message Router ──────────────────────────────────────────────────────
async function handleTextMessage(chatId, userId, text, firstName) {
  const lower = text.toLowerCase();

  // Quick re-invoice: "bill [name] again" or "invoice [name] again"
  const reInvoiceMatch = lower.match(/\b(bill|invoice)\s+(.+?)\s+again\b/i);
  if (reInvoiceMatch && companyProfiles[userId]) {
    const customerName = reInvoiceMatch[2].trim();
    const lastInv      = getLastInvoiceForCustomer(userId, customerName);
    if (lastInv) {
      return handleQuickReInvoice(chatId, userId, customerName, lastInv);
    } else {
      return send(chatId, `❓ No previous invoice found for *${customerName}*.\n\nCreate a new one:\n_"${lastInv ? lastInv.service : 'Service description'} for ${customerName} for [amount]"_`);
    }
  }

  // Expense logging: "spent X on Y" or "expense: X for Y"
  if (/\b(spent|expense[d]?|paid for|cost[s]?)\b/i.test(lower) && /\d+/.test(lower)) {
    if (commandState[userId]?.type === 'expense_confirm') return; // handled elsewhere
    return handleExpenseEntry(chatId, userId, text);
  }

  // Natural language shortcuts
  if (/\b(download|export)\b/i.test(lower)) {
    const period = /this month/i.test(lower) ? 'this_month'
                 : /last month/i.test(lower) ? 'last_month'
                 : /quarter/i.test(lower)    ? 'this_quarter'
                 : /year/i.test(lower)       ? 'this_year' : null;
    if (period) return downloadInvoices(chatId, userId, period);
    return showPeriodSelector(chatId, userId, 'download');
  }
  if (/\b(stat(s|istic)?s?|revenue|earn)/i.test(lower)) {
    const period = /this month/i.test(lower) ? 'this_month'
                 : /last month/i.test(lower) ? 'last_month' : null;
    if (period) return showStats(chatId, userId, period);
    return showPeriodSelector(chatId, userId, 'stats');
  }
  if (/\b(invoice|bill)s?\b/i.test(lower) && !/for.*\d/.test(lower)) return showInvoices(chatId, userId);
  if (/\b(customer|client)s?\b/i.test(lower) && !/for.*\d/.test(lower)) return showCustomers(chatId, userId);
  if (/\b(profile|settings?)\b/i.test(lower)) return showProfile(chatId, userId);

  // AI classification
  const intent = await classifyIntent(text);

  if (intent === 'invoice') {
    await handleInvoiceRequest(chatId, userId, sanitizeInput(text));
  } else if (intent === 'greeting' || intent === 'help') {
    showWelcome(chatId, userId, firstName);
  } else if (intent === 'stats') {
    showPeriodSelector(chatId, userId, 'stats');
  } else if (intent === 'download') {
    showPeriodSelector(chatId, userId, 'download');
  } else {
    send(chatId,
      `❓ I\'m not sure what you mean.\n\n` +
      `To create an invoice, try:\n` +
      `_"Web design for Acme Corp for 3000"_\n\n` +
      `Or use /help to see all commands.`
    );
  }
}

async function handleCommandState(chatId, userId, text) {
  const state = commandState[userId];
  if (!state) return;
  const lower = text.toLowerCase().trim();

  // ── Revenue goal input ───────────────────────────────────────────────────────
  if (state.type === 'set_goal') {
    const amount = parseFloat(text.replace(/[^0-9.]/g, ''));
    if (isNaN(amount) || amount <= 0) {
      return send(chatId, '⚠️ Please enter a valid number, e.g. `10000`.');
    }
    delete commandState[userId];
    setRevenueGoal(userId, amount);
    const currency = companyProfiles[userId]?.currency || 'AED';
    return send(chatId,
      `🎯 *Goal Set!*\n\nMonthly target: *${formatAmount(amount, currency)}*\n\nYour goal will show as a progress bar on your home dashboard. Good luck! 💪`,
      { reply_markup: { inline_keyboard: [[{ text: '🏠 Home', callback_data: 'nav_home' }]] }}
    );
  }

  // ── Template customer name ──────────────────────────────────────────────────
  if (state.type === 'template_customer') {
    const customerName = sanitizeInput(text);
    if (!customerName) return send(chatId, '⚠️ Please enter a valid customer name.');
    delete commandState[userId];

    const { template, subtotal, tax, total, tc } = state;
    const profile = companyProfiles[userId];
    const { pendingInvoices: pi } = require('./core');
    pi[userId] = {
      data: { customer_name: customerName, address: null, line_items: template.line_items },
      profile, subtotal, tax, total, tc,
    };

    const curr = CURRENCIES[profile.currency] || {};
    let preview = `📌 *From Template: ${template.name}*\n`;
    preview += `━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;
    preview += `🏢 *From:*  ${profile.company_name}\n`;
    preview += `👤 *Bill To:*  ${customerName}\n\n`;
    template.line_items.forEach(li => {
      preview += `│ ${li.description} — *${formatAmount(li.amount, profile.currency)}*\n`;
    });
    if (tc.enabled && tax > 0) preview += `\n${tc.type} ${tc.rate}%: ${formatAmount(tax, profile.currency)}\n`;
    preview += `\n💰 *Total: ${formatAmount(total, profile.currency)}*\n\n`;
    preview += `_Tap Generate to create the PDF._`;

    return send(chatId, preview, { reply_markup: { inline_keyboard: [
      [{ text: '✅ Generate Invoice PDF', callback_data: 'confirm_invoice' }],
      [{ text: '❌ Cancel',               callback_data: 'nav_home'        }],
    ]}});
  }

  // ── Template name input ─────────────────────────────────────────────────────
  if (state.type === 'template_name') {
    const templateName = sanitizeInput(text).slice(0, 40);
    if (!templateName) return send(chatId, '⚠️ Please enter a valid template name.');
    delete commandState[userId];

    const lastInv = state.lastInv;
    const profile = companyProfiles[userId];
    const tc      = getTaxConfig(profile);

    // Reconstruct line items from saved invoice
    const subtotal = (parseFloat(lastInv.total) || 0) - (parseFloat(lastInv.tax_amount) || 0);
    const template = {
      name:       templateName,
      line_items: [{ description: lastInv.service || 'Services', amount: subtotal > 0 ? subtotal : parseFloat(lastInv.total) }],
      savedAt:    new Date().toISOString(),
    };

    const res = saveTemplate(userId, template);
    if (res.error === 'max_templates') {
      return send(chatId, '⚠️ You have 10 templates (max). Delete one first with /templates.');
    }
    return send(chatId,
      `📌 *Template Saved: "${templateName}"*\n\nUse it anytime with /templates — one tap invoicing! ⚡`,
      { reply_markup: { inline_keyboard: [[{ text: '📌 View Templates', callback_data: 'nav_templates' }]] }}
    );
  }

  // ── Period-based commands (stats, download) ─────────────────────────────────
  const period = /this month/i.test(lower)  ? 'this_month'
               : /last month/i.test(lower)  ? 'last_month'
               : /quarter/i.test(lower)     ? 'this_quarter'
               : /this year/i.test(lower)   ? 'this_year'
               : /\ball\b/i.test(lower)     ? 'all' : null;
  if (period) {
    delete commandState[userId];
    if (state.type === 'stats') showStats(chatId, userId, period);
    else downloadInvoices(chatId, userId, period);
  } else {
    send(chatId, '⚠️ Please use the buttons, or type: "this month", "last month", "this quarter", "this year", or "all".');
  }
}

// ─── Invoice Flow ─────────────────────────────────────────────────────────────
async function handleInvoiceRequest(chatId, userId, text) {
  if (!companyProfiles[userId]) {
    return send(chatId, '⚠️ Please set up your profile first with /setup before creating invoices.');
  }

  await send(chatId, '⚡ Reading your invoice details...');

  const result = await processInvoiceText(userId, text);

  if (result.error === 'no_profile') return send(chatId, '⚠️ Please set up your profile first with /setup.');
  if (result.error === 'parse_failed') {
    return send(chatId,
      '⚠️ Couldn\'t parse invoice details. Try:\n_"[Service] for [Customer] for [Amount]"_\n\nExample: _"Web design for John Smith for 2000"_'
    );
  }
  if (result.error === 'validation') {
    return send(chatId,
      `⚠️ *Missing info:*\n${result.errors.map(e => `• ${e}`).join('\n')}\n\n` +
      `_Example: "Consulting for Ahmed Al-Rashidi for 1500"_`
    );
  }

  const { pending } = result;
  const { data, profile, subtotal, tax, total, tc } = pending;
  const curr = CURRENCIES[profile.currency] || {};

  let preview = `📋 *Invoice Preview*\n`;
  preview += `━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;
  preview += `🏢 *From:*  ${profile.company_name}\n`;
  preview += `👤 *Bill To:*  ${data.customer_name}\n`;
  if (data.address && data.address !== 'null' && data.address?.trim()) {
    preview += `📍  ${data.address}\n`;
  }
  preview += `\n`;
  preview += `┌─ *Services ─────────────────┐*\n`;
  data.line_items.forEach(item => {
    const amtStr = formatAmount(item.amount, profile.currency);
    // Pad description for alignment
    preview += `│ ${item.description}\n`;
    preview += `│   → *${amtStr}*\n`;
  });
  preview += `└──────────────────────────────┘\n\n`;

  if (data.line_items.length > 1 || tc.enabled) {
    if (data.line_items.length > 1) {
      preview += `Subtotal:  ${formatAmount(subtotal, profile.currency)}\n`;
    }
    if (tc.enabled && tax > 0) {
      preview += `${tc.type} ${tc.rate}%:  ${formatAmount(tax, profile.currency)}\n`;
    }
    preview += `\n`;
  }
  preview += `💰 *Total Due:  ${formatAmount(total, profile.currency)}*\n\n`;
  preview += `${curr.flag || ''} ${profile.currency}  ·  ${new Date().toLocaleDateString('en-GB')}\n\n`;
  preview += `_Looks good? Tap Generate to create your PDF._`;

  await send(chatId, preview, {
    reply_markup: { inline_keyboard: [
      [{ text: '✅ Generate Invoice PDF', callback_data: 'confirm_invoice' }],
      [{ text: '🔄 Try Again',             callback_data: 'retry_invoice'   }]
    ]}
  });
}

async function handleConfirmInvoice(chatId, userId) {
  if (!pendingInvoices[userId]) return send(chatId, '⚠️ No pending invoice. Please describe your invoice again.');

  await send(chatId, '📄 Generating your PDF invoice...');

  try {
    const result = await confirmInvoice(userId);
    if (result.error) return send(chatId, '⚠️ Error generating invoice. Please try again.');

    let caption =
      `✅ *Invoice Generated!*\n\n` +
      `📄 \`${result.invoiceId}\`\n` +
      `👤 ${result.customer}\n` +
      `💰 *${formatAmount(result.total, result.currency)}*`;
    if (result.paymentUrl) {
      caption += `\n\n💳 *Payment Link:*\n${result.paymentUrl}`;
    }
    caption += `\n\n_Tap ✅ Mark as Paid when your client settles._`;

    await bot.sendDocument(chatId, result.pdfPath, { caption, parse_mode: 'Markdown',
      reply_markup: { inline_keyboard: [
        [{ text: '✅ Mark as Paid',     callback_data: `paid_${result.invoiceId}` }],
        [{ text: '📌 Save as Template', callback_data: 'save_template'            }],
        [
          { text: '📋 All Invoices',   callback_data: 'nav_invoices' },
          { text: '🏠 Home',           callback_data: 'nav_home'     },
        ]
      ]}
    });
    try { fs.unlinkSync(result.pdfPath); } catch (_) {}

  } catch (err) {
    console.error('Invoice confirm error:', err.message);
    send(chatId, '⚠️ Error generating invoice. Please try again.');
  }
}

// ─── Quick Re-Invoice ─────────────────────────────────────────────────────────
async function handleQuickReInvoice(chatId, userId, customerName, lastInv) {
  const profile = companyProfiles[userId];
  const curr    = CURRENCIES[profile.currency] || {};

  let preview = `⚡ *Quick Re-Invoice*\n`;
  preview += `━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;
  preview += `🔄 Based on your last invoice to *${customerName}*\n`;
  preview += `📅 Original: ${lastInv.date}\n\n`;
  preview += `👤 *Bill To:*  ${customerName}\n\n`;
  preview += `┌─ *Services ───────────────────┐*\n`;
  preview += `│ ${lastInv.service || 'Previous service'}\n`;
  preview += `└──────────────────────────────┘\n\n`;
  preview += `💰 *Amount:  ${formatAmount(lastInv.total, lastInv.currency || profile.currency)}*\n\n`;
  preview += `${curr.flag || ''} ${profile.currency}  ·  ${new Date().toLocaleDateString('en-GB')}\n\n`;
  preview += `_Tap Generate to create a new invoice with these details,\nor type a new amount to update it._`;

  // Store a pending invoice based on the last one
  const tc       = getTaxConfig(profile);
  const total    = parseFloat(lastInv.total) || 0;
  const taxAmt   = parseFloat(lastInv.tax_amount) || 0;
  const subtotal = total - taxAmt;

  const { pendingInvoices } = require('./core');
  pendingInvoices[userId] = {
    data: {
      customer_name: customerName,
      address: null,
      line_items: [{ description: lastInv.service || 'Services', amount: subtotal > 0 ? subtotal : total }],
    },
    profile,
    subtotal: subtotal > 0 ? subtotal : total,
    tax: taxAmt,
    total,
    tc,
  };

  await send(chatId, preview, {
    reply_markup: { inline_keyboard: [
      [{ text: '✅ Generate Invoice PDF', callback_data: 'confirm_invoice' }],
      [{ text: '🔄 Different Amount',     callback_data: 'retry_invoice'   }],
    ]}
  });
}

// ─── Invoice Aging Dashboard ──────────────────────────────────────────────────
async function showAgingDashboard(chatId, userId) {
  const invs = invoiceHistory[userId] || [];
  if (invs.length === 0) return send(chatId,
    `⏱ *No Invoices Yet*\n\nCreate your first invoice to start tracking receivables!`);

  const report   = getAgingReport(userId);
  const currency = report.currency;

  let msg = `⏱ *Invoice Aging Report*\n`;
  msg += `━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;

  if (report.count === 0) {
    msg += `✅ *All invoices are paid!*\n\nGreat work — nothing outstanding. 🎉`;
  } else {
    msg += `💰 *Total Outstanding: ${formatAmount(report.totalUnpaid, currency)}*\n`;
    msg += `📄 *${report.count}* unpaid invoice${report.count !== 1 ? 's' : ''}\n\n`;

    for (const [key, bucket] of Object.entries(report.buckets)) {
      if (bucket.invoices.length === 0) continue;
      const bar = asciiBar(bucket.total, report.totalUnpaid || 1, 10);
      msg += `${bucket.emoji} *${bucket.label}*\n`;
      msg += `${bar}  ${formatAmount(bucket.total, currency)}\n`;
      msg += `📄 ${bucket.invoices.length} invoice${bucket.invoices.length !== 1 ? 's' : ''}\n`;

      // Show up to 3 invoices per bucket
      bucket.invoices.slice(0, 3).forEach(inv => {
        msg += `  • \`${inv.invoice_id}\` — ${inv.customer_name} — *${formatAmount(inv.total, inv.currency || currency)}* (${inv.daysOld}d)\n`;
      });
      if (bucket.invoices.length > 3) msg += `  _+${bucket.invoices.length - 3} more_\n`;
      msg += '\n';
    }

    if (report.buckets.days90.invoices.length > 0) {
      msg += `🔴 *Action Required:* ${report.buckets.days90.invoices.length} invoice${report.buckets.days90.invoices.length !== 1 ? 's' : ''} over 90 days — consider escalating collection.\n`;
    }
  }

  await send(chatId, msg, { reply_markup: { inline_keyboard: [
    [
      { text: '📋 All Invoices', callback_data: 'nav_invoices' },
      { text: '📊 Stats',        callback_data: 'nav_stats'    },
    ],
    [{ text: '🏠 Home', callback_data: 'nav_home' }]
  ]}});
}

// ─── Revenue Goals ────────────────────────────────────────────────────────────
async function showGoalSetter(chatId, userId) {
  if (!companyProfiles[userId]) {
    return send(chatId, '⚠️ Please set up your profile first with /setup.');
  }
  const goal     = getRevenueGoal(userId);
  const profile  = companyProfiles[userId];
  const currency = profile.currency;

  const thisMonth  = filterInvoicesByPeriod(invoiceHistory[userId] || [], 'this_month');
  const monthStats = calculateStats(thisMonth, currency);

  let msg = `🎯 *Monthly Revenue Goal*\n`;
  msg += `━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;

  if (goal) {
    const pct = Math.min(100, Math.round((monthStats.total / goal.monthly) * 100));
    const bar = asciiBar(monthStats.total, goal.monthly, 14);
    msg += `*Current Goal:* ${formatAmount(goal.monthly, currency)}/month\n\n`;
    msg += `${bar} ${pct}%\n`;
    msg += `✅ Achieved: ${formatAmount(monthStats.total, currency)}\n`;
    msg += `🎯 Remaining: ${formatAmount(Math.max(0, goal.monthly - monthStats.total), currency)}\n\n`;
    if (pct >= 100) msg += `🎉 *Goal reached!* Excellent work this month!\n\n`;
  } else {
    msg += `You haven\'t set a monthly goal yet.\n\n`;
  }
  msg += `*Set a new monthly goal:*\n_Reply with a number (e.g. \`10000\`)_`;

  commandState[userId] = { type: 'set_goal' };

  await send(chatId, msg, { reply_markup: { inline_keyboard: [
    [
      { text: '📊 View Stats', callback_data: 'nav_stats' },
      { text: '🏠 Home',       callback_data: 'nav_home'  },
    ]
  ]}});
}

// ─── Client Statement ─────────────────────────────────────────────────────────
async function selectClientForStatement(chatId, userId) {
  const invs = invoiceHistory[userId] || [];
  if (invs.length === 0) return send(chatId,
    `📄 *No Invoices Yet*\n\nCreate invoices first to generate client statements.`);

  // Build unique client list
  const clients = [...new Set(invs.map(i => i.customer_name?.trim()).filter(Boolean))];

  if (clients.length === 0) return send(chatId, '⚠️ No clients found.');

  let msg = `📄 *Client Statement PDF*\n`;
  msg += `━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;
  msg += `Select a client to generate their full invoice statement:\n\n`;

  // Show as inline buttons (up to 8 clients)
  const keyboard = clients.slice(0, 8).map(name => ([{
    text: `📄 ${name}`,
    callback_data: `stmt_${name.slice(0, 30)}`,
  }]));
  keyboard.push([{ text: '🏠 Home', callback_data: 'nav_home' }]);

  await send(chatId, msg, { reply_markup: { inline_keyboard: keyboard } });
}

async function handleClientStatement(chatId, userId, customerName) {
  await send(chatId, `📄 Generating statement for *${customerName}*...`);
  try {
    const result = await generateClientStatement(userId, customerName);
    if (!result) return send(chatId, `⚠️ No invoices found for *${customerName}*.`);

    const { pdfPath, invoiceCount, total, paid, outstanding, currency } = result;
    const caption =
      `📄 *Client Statement — ${customerName}*\n\n` +
      `📋 ${invoiceCount} invoice${invoiceCount !== 1 ? 's' : ''}\n` +
      `💰 Total: ${formatAmount(total, currency)}\n` +
      `✅ Paid: ${formatAmount(paid, currency)}\n` +
      `⏳ Outstanding: ${formatAmount(outstanding, currency)}`;

    await bot.sendDocument(chatId, pdfPath, { caption, parse_mode: 'Markdown',
      reply_markup: { inline_keyboard: [
        [
          { text: '📋 All Invoices', callback_data: 'nav_invoices' },
          { text: '🏠 Home',         callback_data: 'nav_home'     },
        ]
      ]}
    });
    try { fs.unlinkSync(pdfPath); } catch (_) {}
  } catch (err) {
    console.error('Statement error:', err.message);
    send(chatId, '⚠️ Error generating statement. Please try again.');
  }
}

// ─── Invoice Templates ────────────────────────────────────────────────────────
async function showTemplates(chatId, userId) {
  const templates = getTemplates(userId);

  let msg = `📌 *Invoice Templates*\n`;
  msg += `━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;

  if (templates.length === 0) {
    msg += `You have no saved templates yet.\n\n`;
    msg += `*How to save a template:*\n`;
    msg += `After creating an invoice, tap *💾 Save as Template*\n\n`;
    msg += `Templates let you re-invoice with one tap — perfect for recurring services.`;
    return send(chatId, msg, { reply_markup: { inline_keyboard: [[{ text: '🏠 Home', callback_data: 'nav_home' }]] }});
  }

  const profile = companyProfiles[userId];
  const currency = profile?.currency || 'AED';

  msg += `${templates.length} template${templates.length !== 1 ? 's' : ''} saved:\n\n`;
  templates.forEach((t, i) => {
    const total = (t.line_items || []).reduce((s, li) => s + (parseFloat(li.amount) || 0), 0);
    msg += `${i + 1}. 📌 *${t.name}*\n`;
    msg += `   ${t.line_items?.map(li => li.description).join(', ')}\n`;
    msg += `   💰 ${formatAmount(total, currency)}\n\n`;
  });

  const keyboard = templates.slice(0, 8).map((t, i) => ([{
    text: `⚡ Use "${t.name}"`,
    callback_data: `tpl_use_${i}`,
  }]));
  keyboard.push([{ text: '🗑 Manage Templates', callback_data: 'tpl_manage' }]);
  keyboard.push([{ text: '🏠 Home', callback_data: 'nav_home' }]);

  await send(chatId, msg, { reply_markup: { inline_keyboard: keyboard } });
}

async function handleTemplateUse(chatId, userId, templateIndex) {
  const templates = getTemplates(userId);
  const template  = templates[templateIndex];
  if (!template) return send(chatId, '⚠️ Template not found.');

  const profile = companyProfiles[userId];
  if (!profile) return send(chatId, '⚠️ Please set up your profile first with /setup.');

  const tc       = getTaxConfig(profile);
  const subtotal = (template.line_items || []).reduce((s, li) => s + (parseFloat(li.amount) || 0), 0);
  const tax      = tc.enabled ? subtotal * (tc.rate / 100) : 0;
  const total    = subtotal + tax;
  const curr     = CURRENCIES[profile.currency] || {};

  let msg = `📌 *Template: ${template.name}*\n`;
  msg += `━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;
  msg += `Who should this invoice go to?\n\n`;
  msg += `*Services:*\n`;
  (template.line_items || []).forEach(li => {
    msg += `• ${li.description} — ${formatAmount(li.amount, profile.currency)}\n`;
  });
  if (tc.enabled) msg += `\n${tc.type} ${tc.rate}%: ${formatAmount(tax, profile.currency)}\n`;
  msg += `\n💰 *Total: ${formatAmount(total, profile.currency)}*\n\n`;
  msg += `_Type the customer name to create the invoice:_`;

  commandState[userId] = { type: 'template_customer', template, subtotal, tax, total, tc };
  await send(chatId, msg);
}

async function handleTemplateManage(chatId, userId) {
  const templates = getTemplates(userId);
  if (templates.length === 0) return send(chatId, '📌 No templates to manage.');

  let msg = `🗑 *Manage Templates*\n\nTap a template to delete it:\n\n`;

  const keyboard = templates.map((t, i) => ([{
    text: `❌ Delete "${t.name}"`,
    callback_data: `tpl_del_${i}`,
  }]));
  keyboard.push([{ text: '◀ Back', callback_data: 'nav_templates' }]);

  await send(chatId, msg, { reply_markup: { inline_keyboard: keyboard } });
}

// ─── Expense Tracker ──────────────────────────────────────────────────────────
async function handleExpenseEntry(chatId, userId, text) {
  if (!companyProfiles[userId]) {
    return send(chatId, '⚠️ Please set up your profile first with /setup.');
  }
  await send(chatId, '💸 _Processing expense..._');
  try {
    const data     = await extractExpenseData(sanitizeInput(text));
    const profile  = companyProfiles[userId];
    const currency = profile.currency;

    if (!data.amount || parseFloat(data.amount) <= 0) {
      return send(chatId, '⚠️ Couldn\'t detect an amount. Try:\n_"Spent 500 on petrol"_\n_"Office supplies 200"_');
    }

    // Store preview
    commandState[userId] = { type: 'expense_confirm', expenseData: data };

    const msg =
      `💸 *Expense Preview*\n` +
      `━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
      `📝 ${data.description}\n` +
      `🏷 Category: *${data.category}*\n` +
      `💰 *${formatAmount(data.amount, currency)}*\n\n` +
      `_Looks right?_`;

    await send(chatId, msg, { reply_markup: { inline_keyboard: [
      [{ text: '✅ Log Expense', callback_data: 'exp_confirm' }],
      [{ text: '❌ Cancel',     callback_data: 'exp_cancel'  }],
    ]}});
  } catch (err) {
    console.error('Expense parse error:', err.message);
    send(chatId, '⚠️ Couldn\'t parse expense. Try:\n_"Spent 300 on software"_\n_"Paid 150 for office supplies"_');
  }
}

async function showExpenses(chatId, userId) {
  const profile = companyProfiles[userId];
  if (!profile) return send(chatId, '⚠️ Please set up your profile first.');

  const expenses = expenseHistory[userId] || [];
  const currency = profile.currency;

  if (expenses.length === 0) {
    return send(chatId,
      `💸 *Expense Tracker*\n━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
      `No expenses logged yet.\n\n` +
      `*Log an expense by typing:*\n` +
      `_"Spent 500 on petrol"_\n` +
      `_"Office supplies 200"_\n` +
      `_"Paid 1500 for subcontractor"_`,
      { reply_markup: { inline_keyboard: [[{ text: '🏠 Home', callback_data: 'nav_home' }]] }}
    );
  }

  const recent    = expenses.slice(-10).reverse();
  const thisMonth = getExpenses(userId, 'this_month');
  const monthTotal= thisMonth.reduce((s, e) => s + (parseFloat(e.amount) || 0), 0);
  const allTotal  = expenses.reduce((s, e) => s + (parseFloat(e.amount) || 0), 0);

  let msg = `💸 *Expense Tracker*\n`;
  msg += `━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;
  msg += `📅 This Month: *${formatAmount(monthTotal, currency)}*\n`;
  msg += `📊 All Time: ${formatAmount(allTotal, currency)}\n\n`;
  msg += `*Recent Expenses:*\n\n`;

  const catEmoji = { Travel: '✈️', Software: '💻', Office: '🏢', Marketing: '📣', Subcontractors: '👷', Equipment: '🔧', Other: '📦' };
  recent.forEach(exp => {
    const icon = catEmoji[exp.category] || '📦';
    msg += `${icon} *${formatAmount(exp.amount, exp.currency || currency)}* — ${exp.description}\n`;
    msg += `   🏷 ${exp.category}  ·  📅 ${exp.date}\n\n`;
  });

  if (expenses.length > 10) msg += `_+${expenses.length - 10} older expenses_\n`;

  await send(chatId, msg, { reply_markup: { inline_keyboard: [
    [
      { text: '📈 P&L Report',   callback_data: 'nav_profit'  },
      { text: '📊 Stats',        callback_data: 'nav_stats'   },
    ],
    [{ text: '🏠 Home', callback_data: 'nav_home' }]
  ]}});
}

async function showProfitLoss(chatId, userId, period) {
  const profile = companyProfiles[userId];
  if (!profile) return send(chatId, '⚠️ Please set up your profile first.');

  const pl       = calculateProfitLoss(userId, period);
  const currency = profile.currency;
  const isProfit = pl.profit >= 0;

  let msg = `📈 *Profit & Loss — ${PERIOD_NAMES[period] || period}*\n`;
  msg += `━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;
  msg += `💰 Revenue:   *${formatAmount(pl.revenue, currency)}*\n`;
  msg += `💸 Expenses:  *${formatAmount(pl.expenses, currency)}*\n`;
  msg += `\n`;
  msg += `${isProfit ? '✅' : '🔴'} *${isProfit ? 'Profit' : 'Loss'}:  ${formatAmount(Math.abs(pl.profit), currency)}*\n`;
  msg += `📊 Margin:  ${pl.margin.toFixed(1)}%\n\n`;

  if (Object.keys(pl.byCategory).length > 0) {
    const maxExp = Math.max(...Object.values(pl.byCategory));
    msg += `━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;
    msg += `🏷 *Expense Breakdown*\n\n`;
    const catEmoji = { Travel: '✈️', Software: '💻', Office: '🏢', Marketing: '📣', Subcontractors: '👷', Equipment: '🔧', Other: '📦' };
    Object.entries(pl.byCategory).sort((a, b) => b[1] - a[1]).forEach(([cat, amt]) => {
      const bar  = asciiBar(amt, maxExp || 1, 10);
      const icon = catEmoji[cat] || '📦';
      msg += `${icon} *${cat}*\n   ${bar}  ${formatAmount(amt, currency)}\n\n`;
    });
  }

  if (pl.invoiceCount === 0 && pl.expenseCount === 0) {
    msg += `_No data for this period. Try "All Time"._\n`;
  }

  await send(chatId, msg, { reply_markup: { inline_keyboard: [
    [
      { text: '💸 Log Expense', callback_data: 'nav_expenses'    },
      { text: '📊 Revenue',     callback_data: 'nav_stats'       },
    ],
    [
      { text: '📅 This Month',  callback_data: 'profit_this_month'  },
      { text: '📅 This Year',   callback_data: 'profit_this_year'   },
    ],
    [{ text: '🏠 Home', callback_data: 'nav_home' }]
  ]}});
}

// ─── AI Insights Handler ──────────────────────────────────────────────────────
async function handleAIInsights(chatId, userId, period) {
  const invs = invoiceHistory[userId] || [];
  if (invs.length === 0) return send(chatId, '⚠️ No invoice data to analyze yet.');

  const filtered = filterInvoicesByPeriod(invs, period);
  if (filtered.length === 0) return send(chatId, `⚠️ No invoices for ${PERIOD_NAMES[period] || period}.`);

  await send(chatId, '🤖 _Analyzing your business data..._');

  const currency = companyProfiles[userId]?.currency || 'AED';
  const stats    = calculateStats(filtered, currency);
  const insight  = await generateBusinessInsights(userId, stats, PERIOD_NAMES[period] || period);

  if (!insight) return send(chatId, '⚠️ Couldn\'t generate insights right now. Please try again.');

  const msg =
    `🤖 *AI Business Insights*\n` +
    `📅 ${PERIOD_NAMES[period] || period}\n` +
    `━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
    `${insight}\n\n` +
    `_Powered by Claude AI_`;

  await send(chatId, msg, { reply_markup: { inline_keyboard: [
    [
      { text: '📊 Full Stats',   callback_data: `stats_${period}` },
      { text: '🎯 Set Goal',     callback_data: 'nav_goal'        },
    ],
    [{ text: '🏠 Home', callback_data: 'nav_home' }]
  ]}});
}

// ─── Start Function ───────────────────────────────────────────────────────────
function startTelegramBot() {
  if (!TELEGRAM_TOKEN) {
    console.warn('⚠️  TELEGRAM_TOKEN not set — Telegram bot disabled.');
    return;
  }

  // Initialize bot now that we have a token
  bot = new TelegramBot(TELEGRAM_TOKEN, { polling: true });

  // Re-attach handlers now that bot is initialized
  bot.on('polling_error', (err) => console.error('Polling error:', err.message));

  // Message handler
  bot.on('message', async (msg) => {
    const chatId    = msg.chat.id;
    const userId    = String(msg.from.id);
    const text      = msg.text || '';
    const firstName = msg.from.first_name || 'there';

    if (!checkRateLimit(userId)) {
      return send(chatId, '⏱ You\'re sending too fast — please wait a moment.');
    }

    try {
      if (text.startsWith('/')) { await handleCommand(chatId, userId, text, firstName); return; }
      if (msg.photo && onboardingState[userId]?.step === 'logo') { await handleLogoUpload(chatId, userId, msg.photo); return; }
      if (!companyProfiles[userId] && !onboardingState[userId]) { await showLanding(chatId, firstName); return; }
      if (msg.voice) { await handleVoiceMessage(chatId, userId, msg.voice, firstName); return; }
      if (onboardingState[userId]) { await handleOnboarding(chatId, userId, text); return; }
      if (commandState[userId]) { await handleCommandState(chatId, userId, text); return; }
      if (text) await handleTextMessage(chatId, userId, text, firstName);
    } catch (err) {
      console.error('Message handler error:', err.message);
      send(chatId, '⚠️ Something went wrong. Please try again or use /help.');
    }
  });

  // Callback query handler
  bot.on('callback_query', async (query) => {
    const chatId    = query.message.chat.id;
    const userId    = String(query.from.id);
    const data      = query.data;
    const firstName = query.from.first_name || 'there';

    await bot.answerCallbackQuery(query.id).catch(() => {});
    if (!checkRateLimit(userId)) return;

    try {
      if      (data === 'cmd_setup')         startOnboarding(chatId, userId, firstName);
      else if (data === 'cmd_help')          showHelp(chatId);
      else if (data === 'setup_agree')       handleOnboarding(chatId, userId, 'agree');
      else if (data === 'setup_cancel') {
        delete onboardingState[userId];
        send(chatId, '❌ Setup cancelled. Use /setup to restart any time.');
      }
      else if (data === 'setup_skip')        handleOnboarding(chatId, userId, 'skip');
      else if (data.startsWith('currency_')) handleOnboarding(chatId, userId, data.replace('currency_', ''));
      else if (data === 'currency_more')     showCurrencyPage(chatId, userId, 1);
      else if (data === 'currency_back')     showCurrencyPage(chatId, userId, 0);
      else if (data === 'tax_yes')           handleOnboarding(chatId, userId, 'yes');
      else if (data === 'tax_no')            handleOnboarding(chatId, userId, 'no');
      else if (data.startsWith('stats_'))    showStats(chatId, userId, data.replace('stats_', ''));
      else if (data.startsWith('dl_'))       downloadInvoices(chatId, userId, data.replace('dl_', ''));
      else if (data === 'confirm_invoice')   handleConfirmInvoice(chatId, userId);
      else if (data === 'retry_invoice') {
        delete pendingInvoices[userId];
        send(chatId, '🔄 Let\'s try again.\n\nDescribe your invoice:\n_"Plumbing for Ahmed at Marina for 500"_');
      }
      else if (data === 'nav_home')          showWelcome(chatId, userId, firstName);
      else if (data === 'nav_invoices')      showInvoices(chatId, userId);
      else if (data === 'nav_stats')         showPeriodSelector(chatId, userId, 'stats');
      else if (data === 'nav_profile')       showProfile(chatId, userId);
      else if (data === 'nav_download')      showPeriodSelector(chatId, userId, 'download');
      else if (data === 'nav_customers')     showCustomers(chatId, userId);
      else if (data === 'nav_aging')         showAgingDashboard(chatId, userId);
      else if (data === 'nav_goal')          showGoalSetter(chatId, userId);
      else if (data === 'nav_templates')     showTemplates(chatId, userId);
      else if (data === 'nav_expenses')      showExpenses(chatId, userId);
      else if (data === 'nav_profit')        showProfitLoss(chatId, userId, 'this_month');
      else if (data === 'nav_statement')     selectClientForStatement(chatId, userId);
      else if (data.startsWith('stmt_'))     handleClientStatement(chatId, userId, data.replace('stmt_', ''));
      else if (data.startsWith('insights_')) handleAIInsights(chatId, userId, data.replace('insights_', ''));
      else if (data.startsWith('profit_'))   showProfitLoss(chatId, userId, data.replace('profit_', ''));
      else if (data.startsWith('tpl_use_'))  handleTemplateUse(chatId, userId, parseInt(data.replace('tpl_use_', '')));
      else if (data === 'tpl_manage')        handleTemplateManage(chatId, userId);
      else if (data.startsWith('tpl_del_')) {
        const idx = parseInt(data.replace('tpl_del_', ''));
        const templates = getTemplates(userId);
        if (templates[idx]) {
          deleteTemplate(userId, templates[idx].name);
          send(chatId, `🗑 Template "*${templates[idx].name}*" deleted.`, { reply_markup: { inline_keyboard: [[{ text: '📌 Templates', callback_data: 'nav_templates' }]] }});
        }
      }
      else if (data === 'exp_confirm') {
        const state = commandState[userId];
        if (state?.type === 'expense_confirm' && state.expenseData) {
          const expense = logExpense(userId, state.expenseData);
          delete commandState[userId];
          const currency = companyProfiles[userId]?.currency || 'AED';
          send(chatId,
            `✅ *Expense Logged!*\n\n📝 ${expense.description}\n🏷 ${expense.category}\n💰 *${formatAmount(expense.amount, currency)}*\n\n_Tap P&L to see your profit margin._`,
            { reply_markup: { inline_keyboard: [
              [{ text: '📈 P&L Report', callback_data: 'nav_profit' }],
              [{ text: '🏠 Home',       callback_data: 'nav_home'   }],
            ]}}
          );
        }
      }
      else if (data === 'exp_cancel') {
        delete commandState[userId];
        send(chatId, '❌ Expense cancelled.');
      }
      else if (data === 'save_template') {
        const lastInv = (invoiceHistory[userId] || []).slice(-1)[0];
        if (!lastInv) return send(chatId, '⚠️ No invoice to save as template.');
        commandState[userId] = { type: 'template_name', lastInv };
        send(chatId, `📌 *Save as Template*\n\nGive this template a name:\n_e.g. "Monthly Retainer", "Web Design", "Consulting"_`);
      }
      else if (data.startsWith('paid_'))     handleMarkPaid(chatId, userId, data.replace('paid_', ''));
      else if (data === 'deletedata_confirm') {
        delete companyProfiles[userId];
        delete invoiceHistory[userId];
        saveData();
        send(chatId, '🗑 All data deleted.\n\nUse /setup to start fresh any time. 👋');
      }
      else if (data === 'deletedata_cancel') {
        send(chatId, '✅ *Nothing was deleted.* Your data is safe! 🔒', { reply_markup: { inline_keyboard: [[{ text: '🏠 Home', callback_data: 'nav_home' }]] }});
      }
    } catch (err) {
      console.error('Callback error:', err.message);
      send(chatId, '⚠️ Something went wrong. Please try again.');
    }
  });

  console.log('✅ Telegram bot started (polling)');
}

// ─── Notify helper (used by scheduler) ───────────────────────────────────────
async function telegramNotify(userId, message, opts = {}) {
  if (!bot) return;
  try {
    await bot.sendMessage(parseInt(userId), message, { parse_mode: 'Markdown', ...opts });
  } catch (err) {
    console.error(`Scheduler notify error for ${userId}:`, err.message);
  }
}

module.exports = { startTelegramBot, telegramNotify };
