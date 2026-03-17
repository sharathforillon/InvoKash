/**
 * InvoKash - Telegram Bot (v2)
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
  servicesCatalogue, quoteHistory, clientDirectory, recurringInvoices, creditNotes, brandingSettings,
  CURRENCIES, PERIOD_NAMES, LOGO_DIR, RECEIPTS_DIR, EXPENSE_CATEGORIES, BRANDING_COLORS,
  checkRateLimit, sanitizeInput, formatAmount, getTaxConfig,
  filterInvoicesByPeriod, progressBar, asciiBar, calculateStats,
  classifyIntent, transcribeAudio, processInvoiceText, confirmInvoice,
  markInvoicePaid, buildDownloadZip, buildExpenseZip, saveData,
  getLastInvoiceForCustomer, getAgingReport,
  setRevenueGoal, getRevenueGoal,
  generateBusinessInsights, generateClientStatement,
  saveTemplate, getTemplates, deleteTemplate,
  extractExpenseData, extractExpenseFromImage, extractExpenseFromPDF, logExpense, getExpenses, calculateProfitLoss,
  // v2.2 features
  addService, getServices, deleteService,
  createQuote, getQuotes, convertQuoteToInvoice,
  saveClientWhatsApp, getClientWhatsApp, listClients, deleteClient,
  saveClientEmail, getClientEmail, sendInvoiceEmail,
  createRecurring, getRecurring, pauseRecurring, deleteRecurring, processRecurringInvoices,
  recordPartialPayment, getInvoicePayments,
  generateTaxReport,
  generateCashFlowForecast,
  createCreditNote, getCreditNotes,
  saveBranding, getBranding, resetBranding,
} = require('./core');

// ─── Bot Init ─────────────────────────────────────────────────────────────────
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;

// Bot is initialized lazily in startTelegramBot()
let bot;

// ─── Persistent Quick-Action Keyboard ─────────────────────────────────────────
// Two primary actions always pinned at the bottom of the Telegram chat.
// Set once per user; Telegram keeps it visible until explicitly removed.
const mainKbUsers = new Set();

function ensureMainKeyboard(chatId, userId) {
  if (mainKbUsers.has(userId)) return;
  mainKbUsers.add(userId);
  bot.sendMessage(chatId,
    '📌 _Quick-action shortcuts pinned below - or just type anytime._',
    {
      parse_mode:   'Markdown',
      reply_markup: {
        keyboard:          [[{ text: '📄 New Invoice' }, { text: '💸 Log Expense' }]],
        resize_keyboard:   true,
        one_time_keyboard: false,
      },
    }
  ).catch(() => {});
}

// ─── Onboarding Config ────────────────────────────────────────────────────────
const ONBOARD_TOTAL = 11;

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
    case '/services':  return showServices(chatId, userId);
    case '/quotes':    return showQuotes(chatId, userId);
    case '/clients':   return showClients(chatId, userId);
    case '/recurring': return showRecurring(chatId, userId);
    case '/vat':
    case '/taxreport': return showVatReportSelector(chatId, userId);
    case '/forecast':  return showCashFlowForecast(chatId, userId);
    case '/credits':   return showCreditNotes(chatId, userId);
    case '/branding':  return showBrandingSettings(chatId, userId);
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
    `👋 *Hi ${firstName}! I'm InvoKash.*\n\n` +
    `Create professional invoices in seconds - just type or speak naturally.\n\n` +
    `💬 _"Web design for Ahmed for 3,500"_\n` +
    `🎤 Or send a voice message - works too!\n\n` +
    `↳ Instant PDF · Payment link · Auto reminders\n\n` +
    `✅ 14 currencies · VAT/GST · Stats · Templates\n` +
    `⏱ _Takes ~2 minutes to set up_`,
    { reply_markup: { inline_keyboard: [
      [{ text: '🚀 Set Up My Account', callback_data: 'cmd_setup' }],
      [{ text: '❓ How It Works',      callback_data: 'cmd_help'  }]
    ]}}
  );
}

// ─── Primary Action Prompt Screens ────────────────────────────────────────────
function showInvoicePrompt(chatId, userId) {
  if (!companyProfiles[userId]) return send(chatId, '⚠️ Please set up your profile first with /setup.');
  commandState[userId] = { type: 'expect_invoice' };
  send(chatId,
    `📄 *Create Invoice*\n` +
    `━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
    `Just describe what you\'re billing:\n\n` +
    `_"Website design for Ahmed, 5,000"_\n` +
    `_"3 months consulting for TechCorp, 2,500 each"_\n` +
    `_"Logo design + branding for Acme, 3,200"_\n` +
    `_"Invoice Rania 800 for photography"_\n\n` +
    `Or send a 🎤 voice note - I\'ll transcribe it.\n\n` +
    `_I\'ll extract the details, generate a PDF, and create a payment link._`,
    { reply_markup: { inline_keyboard: [
      [{ text: '📌 Use a Template', callback_data: 'nav_templates' }],
      [{ text: '🏠 Home',           callback_data: 'nav_home'      }],
    ]}}
  );
}

function showExpensePrompt(chatId, userId) {
  if (!companyProfiles[userId]) return send(chatId, '⚠️ Please set up your profile first with /setup.');
  commandState[userId] = { type: 'expect_expense' };
  send(chatId,
    `💸 *Log Expense*\n` +
    `━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
    `*Type it:*\n` +
    `_"Spent 250 on Adobe subscription"_\n` +
    `_"Flight to Dubai 850"_\n` +
    `_"Office supplies 120"_\n` +
    `_"Paid 2,000 for subcontractor"_\n\n` +
    `*Or send a file directly:*\n` +
    `📸 Snap a receipt photo\n` +
    `📄 Forward a PDF (flight ticket, hotel booking, invoice)\n\n` +
    `_I\'ll auto-scan it and suggest the category._`,
    { reply_markup: { inline_keyboard: [
      [{ text: '💸 Recent Expenses', callback_data: 'nav_expenses' }],
      [{ text: '🏠 Home',            callback_data: 'nav_home'     }],
    ]}}
  );
}

function showWelcome(chatId, userId, firstName = 'there') {
  delete commandState[userId]; // Always reset on Home — universal escape hatch
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

  // Urgency: count overdue unpaid invoices (older than 7 days)
  const overdueCount = history.filter(inv => {
    if (inv.status === 'paid') return false;
    const parts = inv.date?.split('/');
    if (!parts || parts.length < 3) return false;
    const d = new Date(parseInt(parts[2]), parseInt(parts[1]) - 1, parseInt(parts[0]));
    return Math.floor((Date.now() - d.getTime()) / 86400000) > 7;
  }).length;

  let msg = `🏠 *${profile.company_name}*  ${curr.flag || ''}${profile.currency}\n`;
  msg += `━━━━━━━━━━━━━━━━━━━\n`;

  if (history.length === 0) {
    msg += `\n🌟 *Welcome! Create your first invoice.*\n\n`;
    msg += `Just type or send a 🎤 voice message:\n`;
    msg += `_"Web design for Acme Corp for 3,500"_\n\n`;
    msg += `InvoKash will extract the details, generate a PDF, and create a payment link automatically.`;
  } else {
    msg += `\n📅 *${new Date().toLocaleString('en-US', { month: 'long' })}*\n`;
    msg += `💰 *${formatAmount(monthStats.total, profile.currency)}*`;
    if (trendIcon) msg += `   ${trendIcon}`;
    msg += `\n`;
    msg += `📄 ${monthStats.count} invoice${monthStats.count !== 1 ? 's' : ''}`;
    if (monthStats.unpaid > 0) msg += `  ·  ⏳ ${formatAmount(monthStats.unpaid, profile.currency)} awaiting payment`;
    msg += `\n`;
    if (allStats.count > monthStats.count) {
      msg += `_All time: ${formatAmount(allStats.total, profile.currency)} · ${allStats.count} invoices_\n`;
    }

    // Revenue goal
    const goal = getRevenueGoal(userId);
    if (goal && goal.monthly > 0) {
      const pct = Math.min(100, Math.round((monthStats.total / goal.monthly) * 100));
      const bar = asciiBar(monthStats.total, goal.monthly, 10);
      const goalMsg = pct >= 100 ? `🏆 *Goal smashed!*` : `🎯 Goal`;
      msg += `\n${goalMsg}  ${bar} ${pct}%\n`;
      msg += `_${formatAmount(monthStats.total, profile.currency)} of ${formatAmount(goal.monthly, profile.currency)}_\n`;
    }

    // Urgency banner
    if (overdueCount > 0) {
      msg += `\n🔴 *${overdueCount} overdue invoice${overdueCount > 1 ? 's' : ''} need attention* → Aging`;
    }

    msg += `\n\n💬 _Type or 🎤 speak to create a new invoice_`;
  }

  // Pin the quick-action keyboard once per session
  ensureMainKeyboard(chatId, userId);

  // Home screen: primary actions first, then navigation
  send(chatId, msg, {
    reply_markup: { inline_keyboard: [
      // Primary actions - always first
      [
        { text: '📄 New Invoice',  callback_data: 'nav_new_invoice' },
        { text: '💸 Log Expense',  callback_data: 'nav_log_expense' },
      ],
      // Tier 1 - Most used daily
      [
        { text: '📋 Invoices',    callback_data: 'nav_invoices'  },
        { text: '👥 Clients',     callback_data: 'nav_customers' },
      ],
      // Tier 2 - Financial intelligence
      [
        { text: '📊 Stats',       callback_data: 'nav_stats'     },
        { text: '📈 P&L',         callback_data: 'nav_profit'    },
      ],
      [
        { text: '⏱ Aging',       callback_data: 'nav_aging'     },
        { text: '🎯 Goal',        callback_data: 'nav_goal'      },
      ],
      // Tier 3 - Tools
      [
        { text: '📌 Templates',   callback_data: 'nav_templates' },
        { text: '🔄 Recurring',   callback_data: 'nav_recurring' },
      ],
      [
        { text: '👤 Profile',     callback_data: 'nav_profile'   },
        { text: '📥 Export',      callback_data: 'nav_download'  },
      ],
    ]}
  });
}

// ─── Help ─────────────────────────────────────────────────────────────────────
function showHelp(chatId) {
  send(chatId,
    `📖 *How InvoKash Works*\n\n` +

    `*🧾 Create an Invoice*\n` +
    `Just type naturally or send a 🎤 voice note:\n` +
    `_"Web design for Ahmed for 3,000"_\n` +
    `_"Plumbing at Marina for 500 for John Smith"_\n` +
    `→ InvoKash extracts the details, generates a PDF, and creates a Stripe payment link automatically.\n\n` +

    `*⚡ Power Shortcuts*\n` +
    `• _"Bill Ahmed again"_ - re-send last invoice for that client\n` +
    `• _"Spent 200 on software"_ - log a business expense\n` +
    `• 🎤 Voice works in any language\n\n` +

    `*📊 Track Your Money*\n` +
    `📋 Invoices - all your invoices, mark paid here\n` +
    `⏱ Aging - see what's overdue and by how long\n` +
    `📊 Stats - revenue by period with trends\n` +
    `📈 P&L - profit after expenses\n\n` +

    `*🛠 Save Time*\n` +
    `📌 Templates - one-tap invoicing for repeat work\n` +
    `🔄 Recurring - auto-generate invoices on a schedule\n` +
    `📝 Quotes - send a quote first, convert to invoice on approval\n\n` +

    `*💡 Pro tip:* After your 3rd invoice, tap *💾 Save Template* to reuse it in one tap.`,
    { reply_markup: { inline_keyboard: [
      [
        { text: '📋 Invoices',   callback_data: 'nav_invoices'  },
        { text: '⏱ Aging',      callback_data: 'nav_aging'     },
      ],
      [
        { text: '📌 Templates', callback_data: 'nav_templates' },
        { text: '🔄 Recurring', callback_data: 'nav_recurring' },
      ],
      [{ text: '🏠 Home',       callback_data: 'nav_home'      }],
    ]}}
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
      { text: '✅ I Agree - Continue', callback_data: 'setup_agree'  },
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
        `🏢 *Step 1 - Company Name*\n\nWhat is your business or trading name?`
      );
      break;

    case 'company_name':
      if (!text?.trim()) return send(chatId, '⚠️ Please enter a valid company name.');
      p.company_name = sanitizeInput(text);
      state.step = 'company_address';
      send(chatId,
        `${progressBar(2, ONBOARD_TOTAL)}\n\n` +
        `📍 *Step 2 - Business Address*\n\nEnter your full business address:`,
        { reply_markup: { inline_keyboard: [[{ text: '⏭ Skip', callback_data: 'setup_skip' }]] }}
      );
      break;

    case 'company_address':
      p.company_address = input === 'skip' ? '' : sanitizeInput(text);
      state.step = 'trn';
      send(chatId,
        `${progressBar(3, ONBOARD_TOTAL)}\n\n` +
        `🔐 *Step 3 - Tax Registration Number*\n\nEnter your TRN / VAT / GST registration number (optional):`,
        { reply_markup: { inline_keyboard: [[{ text: '⏭ Skip - No TRN', callback_data: 'setup_skip' }]] }}
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
        `🏦 *Step 5 - Bank Name*\n\nEnter your bank name:\n_e.g. Emirates NBD, HDFC, Barclays, Chase_`
      );
      break;
    }

    case 'bank_name':
      p.bank_name = sanitizeInput(text);
      state.step = 'iban';
      send(chatId,
        `${progressBar(6, ONBOARD_TOTAL)}\n\n` +
        `🔑 *Step 6 - ${p.currency === 'INR' ? 'Account Number & IFSC' : 'IBAN'}*\n\n` +
        `Enter your ${p.currency === 'INR' ? 'account number and IFSC code' : 'IBAN'}:`
      );
      break;

    case 'iban':
      p.iban = sanitizeInput(text);
      state.step = 'account_name';
      send(chatId,
        `${progressBar(7, ONBOARD_TOTAL)}\n\n` +
        `👤 *Step 7 - Account Holder Name*\n\nName on the bank account:`
      );
      break;

    case 'account_name':
      p.account_name = sanitizeInput(text);
      state.step = 'owner_email';
      send(chatId,
        `${progressBar(8, ONBOARD_TOTAL)}\n\n` +
        `📧 *Step 8 - Your Business Email*\n\n` +
        `Enter your email address — you'll receive a copy (CC) of every invoice you email to clients.\n\n` +
        `_e.g. ahmed@mybusiness.com_`,
        { reply_markup: { inline_keyboard: [[{ text: '⏭ Skip', callback_data: 'setup_skip' }]] }}
      );
      break;

    case 'owner_email': {
      if (input === 'skip') {
        p.owner_email = '';
      } else {
        const emailInput = text.trim().toLowerCase();
        if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(emailInput)) {
          return send(chatId,
            `⚠️ That doesn't look right. Please enter a valid email address, e.g. _ahmed@mybusiness.com_\n\n` +
            `_(Or tap Skip to do this later)_`,
            { reply_markup: { inline_keyboard: [[{ text: '⏭ Skip', callback_data: 'setup_skip' }]] }}
          );
        }
        p.owner_email = emailInput;
      }
      state.step = 'tax_enabled';
      const taxType = CURRENCIES[p.currency]?.tax || 'VAT';
      send(chatId,
        `${progressBar(9, ONBOARD_TOTAL)}\n\n` +
        `📊 *Step 9 - Tax Settings*\n\nDo you charge *${taxType}* on your invoices?`,
        { reply_markup: { inline_keyboard: [[
          { text: `✅ Yes, I charge ${taxType}`, callback_data: 'tax_yes' },
          { text: '❌ No tax',                   callback_data: 'tax_no'  }
        ]]}}
      );
      break;
    }

    case 'tax_enabled': {
      const taxField = ['INR','SGD','AUD'].includes(p.currency) ? 'gst' : 'vat';
      if (input === 'yes') {
        p[`${taxField}_enabled`] = true;
        if (taxField === 'gst') { p.vat_enabled = false; p.vat_rate = 0; }
        else                    { p.gst_enabled = false; p.gst_rate = 0; }
        state.step = 'tax_rate';
        send(chatId,
          `${progressBar(10, ONBOARD_TOTAL)}\n\n` +
          `📈 *Step 10 - ${taxField.toUpperCase()} Rate*\n\nEnter the percentage (e.g. \`5\` for 5%):`,
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
    `${progressBar(11, ONBOARD_TOTAL)}\n\n` +
    `🖼 *Step 11 - Company Logo (Optional)*\n\nSend your logo as a PNG or JPG, or skip to use text header.`,
    { reply_markup: { inline_keyboard: [[{ text: '⏭ Skip Logo', callback_data: 'setup_skip' }]] }}
  );
}

async function sendSetupComplete(chatId, userId) {
  const p    = companyProfiles[userId];
  const tc   = getTaxConfig(p);
  const curr = CURRENCIES[p.currency] || {};

  await send(chatId,
    `🎉 *You're all set, ${p.company_name}!*\n\n` +
    `${curr.flag || ''} ${p.currency}  ·  ${tc.type}: ${tc.enabled ? `${tc.rate}%` : 'None'}\n` +
    `🏦 ${p.bank_name || '_No bank set_'}  ·  🖼 ${p.logo_path ? 'Logo uploaded' : 'Text header'}\n` +
    (p.owner_email ? `📧 CC emails → ${p.owner_email}\n` : '') +
    `\n*Create your first invoice - just type or 🎤 voice:*\n` +
    `_"Web design for Acme Corp for 3000"_`,
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

  // Mask IBAN - show only last 4 chars
  const maskedIban = p.iban
    ? `****${p.iban.replace(/\s/g, '').slice(-4)}`
    : '_Not set_';

  send(chatId,
    `👤 *${p.company_name}*\n` +
    `📍 ${p.company_address || '_Not set_'}\n` +
    `${curr.flag || '🌍'} ${p.currency}  ·  ${tc.type}: ${tc.enabled ? `${tc.rate}%` : 'None'}\n` +
    (p.trn ? `🔐 TRN: \`${p.trn}\`\n` : '') +
    (p.owner_email ? `📧 ${p.owner_email}\n` : `📧 _Email not set — tap Update to add_\n`) +
    `\n🏦 *Bank*\n` +
    `${p.bank_name || '_Not set_'}  ·  ${maskedIban}\n` +
    `${p.account_name || ''}\n` +
    `🖼 Logo: ${p.logo_path ? '✅' : '⬜ Text header'}\n\n` +
    `📈 *Performance*\n` +
    `${stats.count} invoices  ·  ${formatAmount(stats.total, p.currency)}\n` +
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
      `Create your first invoice - just type naturally:\n\n` +
      `_"Consulting for John Smith for 1,500"_\n` +
      `_"Web design for Acme Corp for 3,000"_\n\n` +
      `Or send a 🎤 voice message. InvoKash does the rest.`
    );
  }

  const currency  = companyProfiles[userId]?.currency || 'AED';
  const recent    = invs.slice(-10).reverse();
  const unpaidAll = invs.filter(i => i.status !== 'paid');

  // Helper: relative date
  const relDate = (dateStr) => {
    if (!dateStr) return '';
    const parts = dateStr.split('/');
    if (parts.length < 3) return dateStr;
    const d    = new Date(parseInt(parts[2]), parseInt(parts[1]) - 1, parseInt(parts[0]));
    const days = Math.floor((Date.now() - d.getTime()) / 86400000);
    if (days === 0) return 'Today';
    if (days === 1) return 'Yesterday';
    if (days < 7)  return `${days}d ago`;
    if (days < 30) return `${Math.floor(days / 7)}w ago`;
    return dateStr;
  };

  const recentUnpaid = recent.filter(i => i.status !== 'paid');
  const recentPaid   = recent.filter(i => i.status === 'paid');

  let msg = `📋 *Invoices*\n`;

  // ── Awaiting payment ─────────────────────────────────────
  if (recentUnpaid.length > 0) {
    msg += `\n⏳ *Awaiting Payment`;
    if (unpaidAll.length > recentUnpaid.length) msg += ` (${unpaidAll.length} total)`;
    msg += `*\n`;
    recentUnpaid.forEach(inv => {
      const customer  = inv.customer_name?.trim() || 'Unknown';
      const amount    = formatAmount(parseFloat(inv.total) || 0, inv.currency || currency);
      const shortId   = inv.invoice_id.replace('INV-', '');
      if (inv.status === 'partial') {
        const remaining = formatAmount(parseFloat(inv.remaining) || 0, inv.currency || currency);
        msg += `▸ *${customer}*  💛 Partial - *${remaining} left*\n`;
        msg += `   #${shortId}  ·  ${amount} total  ·  ${relDate(inv.date)}\n`;
      } else {
        msg += `▸ *${customer}*  💰 *${amount}*\n`;
        msg += `   #${shortId}  ·  ${relDate(inv.date)}\n`;
      }
    });
  }

  // ── Collected ────────────────────────────────────────────
  if (recentPaid.length > 0) {
    msg += `\n✅ *Collected*\n`;
    recentPaid.forEach(inv => {
      const customer = inv.customer_name?.trim() || 'Unknown';
      const amount   = formatAmount(parseFloat(inv.total) || 0, inv.currency || currency);
      const shortId  = inv.invoice_id.replace('INV-', '');
      msg += `▸ ${customer}  ${amount}  ·  #${shortId}  ·  ${relDate(inv.date)}\n`;
    });
  }

  if (invs.length > 10) msg += `\n_Showing 10 most recent · Download for full history_\n`;

  // ── Action buttons - invoice ref + amount baked into each button label ──────────
  const keyboard = [];
  const unpaidForButtons = recentUnpaid.slice(0, 3);

  if (unpaidForButtons.length > 0) {
    unpaidForButtons.forEach(inv => {
      const customer = inv.customer_name?.trim() || 'Client';
      const amount   = formatAmount(parseFloat(inv.remaining || inv.total) || 0, inv.currency || currency);
      const shortId  = inv.invoice_id.replace(`INV-${new Date().getFullYear()}-`, '#').replace(/^INV-\d{4}-/, '#').replace('INV-', '#');
      // Context row — who, how much, which invoice (full width, tapping does nothing)
      keyboard.push([
        { text: `📄 ${customer}  ${amount}  ${shortId}`, callback_data: `noop_${inv.invoice_id}` }
      ]);
      // Each action on its own full-width row — no truncation, crystal clear
      keyboard.push([
        { text: `✅ Mark Paid — ${customer}`, callback_data: `paid_${inv.invoice_id}` }
      ]);
      keyboard.push([
        { text: `💰 Part Payment — ${customer}`, callback_data: `partial_${inv.invoice_id}` }
      ]);
    });
  }

  keyboard.push([
    { text: '📥 Export',  callback_data: 'nav_download' },
    { text: '📊 Stats',   callback_data: 'nav_stats'    },
  ]);
  keyboard.push([{ text: '🏠 Home', callback_data: 'nav_home' }]);

  await send(chatId, msg, { reply_markup: { inline_keyboard: keyboard } });
}

// ─── Mark Invoice Paid ────────────────────────────────────────────────────────
function handleMarkPaid(chatId, userId, invoiceId, queryId) {
  if (queryId) {
    bot.answerCallbackQuery(queryId, { text: '💰 Payment recorded!', show_alert: false }).catch(() => {});
  }

  const result = markInvoicePaid(userId, invoiceId);
  if (result) {
    const inv      = (invoiceHistory[userId] || []).find(i => i.invoice_id === invoiceId);
    const customer = inv?.customer_name || 'Client';
    const amount   = inv ? formatAmount(inv.total, inv.currency) : '';

    // Running month total for motivation
    const currency    = companyProfiles[userId]?.currency || 'AED';
    const thisMonth   = filterInvoicesByPeriod(invoiceHistory[userId] || [], 'this_month');
    const monthPaid   = thisMonth.filter(i => i.status === 'paid').reduce((s, i) => s + (parseFloat(i.total) || 0), 0);

    send(chatId,
      `💰 *Ka-ching! ${customer} paid.*\n\n` +
      `${amount ? `*${amount}* landed in your account ✅` : `${invoiceId} marked paid ✅`}\n\n` +
      `📅 You've collected *${formatAmount(monthPaid, currency)}* this month so far.\n\n` +
      `_Any project expenses to log against this payment?_`,
      { reply_markup: { inline_keyboard: [
        [
          { text: '💸 Log Expense',  callback_data: 'nav_log_expense' },
          { text: '📄 New Invoice',  callback_data: 'nav_new_invoice' },
        ],
        [
          { text: '📋 Invoices', callback_data: 'nav_invoices' },
          { text: '🏠 Home',     callback_data: 'nav_home'     },
        ],
      ]}}
    );
  } else {
    send(chatId,
      `⚠️ Invoice \`${invoiceId}\` not found.\n\n_Use 📋 Invoices to see your current invoice list._`,
      { reply_markup: { inline_keyboard: [[{ text: '📋 Invoices', callback_data: 'nav_invoices' }]] }}
    );
  }
}

// ─── Customers ────────────────────────────────────────────────────────────────
function showCustomers(chatId, userId) {
  const invs = invoiceHistory[userId] || [];
  if (invs.length === 0) return send(chatId,
    `👥 *No Clients Yet*\n\nCreate your first invoice to build your client directory!\n\n_"Consulting for John Smith for 1500"_`);

  const currency = companyProfiles[userId]?.currency || 'AED';
  const customers = {};

  invs.forEach(inv => {
    const name = inv.customer_name?.trim();
    if (!name) return;
    if (!customers[name]) customers[name] = { count: 0, total: 0, paid: 0, currency: inv.currency || currency };
    customers[name].count++;
    const total = parseFloat(inv.total) || 0;
    customers[name].total += total;
    if (inv.status === 'paid') {
      customers[name].paid += total;
    } else if (inv.remaining !== undefined) {
      customers[name].paid += Math.max(0, total - parseFloat(inv.remaining));
    }
  });

  const sorted         = Object.entries(customers).sort((a, b) => b[1].total - a[1].total);
  const totalBilled    = sorted.reduce((s, [, d]) => s + d.total, 0);
  const totalCollected = sorted.reduce((s, [, d]) => s + d.paid,  0);
  const totalOwed      = totalBilled - totalCollected;

  // Two groups: outstanding balance vs fully cleared
  const outstanding = sorted
    .filter(([, d]) => d.paid < d.total)
    .sort((a, b) => (b[1].total - b[1].paid) - (a[1].total - a[1].paid));
  const cleared = sorted.filter(([, d]) => d.paid >= d.total && d.total > 0);

  // ── Portfolio summary ────────────────────────────────────────────────────────
  let msg = `👥 *Client Overview*  ·  ${sorted.length} clients\n`;
  msg += `━━━━━━━━━━━━━━━━━━━\n`;
  msg += `📋 Billed        *${formatAmount(totalBilled, currency)}*\n`;
  msg += `✅ Collected     *${formatAmount(totalCollected, currency)}*\n`;
  if (totalOwed > 0.009) {
    msg += `🔴 Outstanding   *${formatAmount(totalOwed, currency)}*\n`;
  } else {
    msg += `🎉 All invoices cleared!\n`;
  }

  // ── Clients with outstanding balance ─────────────────────────────────────────
  if (outstanding.length > 0) {
    msg += `\n*NEEDS ATTENTION  (${outstanding.length})*\n`;
    outstanding.forEach(([name, d]) => {
      const owed    = d.total - d.paid;
      const paidPct = Math.round((d.paid / d.total) * 100);
      const icon    = paidPct === 0 ? '⚫' : paidPct < 50 ? '🔴' : '🟡';
      msg += `\n${icon} *${name}*\n`;
      msg += `   💸 *${formatAmount(owed, d.currency)}* outstanding\n`;
      msg += `   ${formatAmount(d.total, d.currency)} billed  ·  ${d.count} inv`;
      if (d.paid > 0) msg += `  ·  ${paidPct}% paid`;
      msg += `\n`;
    });
  }

  // ── Fully paid clients ───────────────────────────────────────────────────────
  if (cleared.length > 0) {
    msg += `\n*ALL CLEAR  (${cleared.length})*\n`;
    cleared.forEach(([name, d]) => {
      msg += `✅ *${name}*  -  ${formatAmount(d.total, d.currency)}  ·  ${d.count} inv\n`;
    });
  }

  if (sorted.length > 10) msg += `\n_+${sorted.length - 10} more clients in export_`;

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
      [{ text: '📅 All Time',      callback_data: `${prefix}all`          }],
      [{ text: '🏠 Home',          callback_data: 'nav_home'              }]
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

  let msg = `📊 *${PERIOD_NAMES[period] || period}*\n\n`;

  msg += `💰 *${formatAmount(stats.total, currency)}*  ·  ${stats.count} invoice${stats.count !== 1 ? 's' : ''}\n`;
  msg += `📈 Avg: ${formatAmount(stats.avg, currency)}\n`;
  if (stats.taxTotal > 0) msg += `🏛 Tax collected: ${formatAmount(stats.taxTotal, currency)}\n`;
  msg += `\n`;

  msg += `*Collection*\n`;
  msg += `${paidBar} ${paidPct}%\n`;
  msg += `✅ ${formatAmount(stats.paid, currency)}  ·  ⏳ ${formatAmount(stats.unpaid, currency)}\n`;

  if (stats.topCustomers.length > 0) {
    msg += `\n*Top Clients*\n`;
    stats.topCustomers.forEach(([name, amt], i) => {
      const bar = asciiBar(amt, stats.topCustomers[0][1] || 1, 6);
      msg += `${i + 1}. *${name}*  ${bar}  ${formatAmount(amt, currency)}\n`;
    });
  }

  // 6-month sparkline (compact)
  if (monthlyData.some(m => m.total > 0)) {
    msg += `\n*Last 6 Months*\n`;
    monthlyData.forEach(({ month, total }) => {
      const bar = asciiBar(total, maxMonth, 8);
      const amt = total > 0 ? formatAmount(total, currency) : 'no data';
      msg += `\`${month}\` ${bar} ${amt}\n`;
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

// ─── Expense Export Period Picker ────────────────────────────────────────────
function showExpenseDownloadPicker(chatId, userId) {
  const expenses = expenseHistory[userId] || [];
  if (expenses.length === 0) {
    return send(chatId,
      `💸 No expenses recorded yet.\n\nStart by logging an expense or sending a 📸 receipt photo.`,
      { reply_markup: { inline_keyboard: [[{ text: '📋 All Expenses', callback_data: 'nav_expenses' }]] }}
    );
  }
  send(chatId,
    `📥 *Download Expenses*\n` +
    `━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
    `Which period do you need?\n\n` +
    `_You'll get a ZIP with:_\n` +
    `• \`expenses.csv\` — ready to share with your accountant\n` +
    `• \`receipts/\` folder — all scanned receipts matched by row\n\n` +
    `_Columns: Date · Description · Merchant · Category · Amount · Currency · Tax Amount · Deductible · Receipt File_`,
    { reply_markup: { inline_keyboard: [
      [{ text: '📅 This Month',    callback_data: 'exp_dl_this_month'   }],
      [{ text: '📅 Last Month',    callback_data: 'exp_dl_last_month'   }],
      [{ text: '📅 This Quarter',  callback_data: 'exp_dl_this_quarter' }],
      [{ text: '📅 This Year',     callback_data: 'exp_dl_this_year'    }],
      [{ text: '📦 All Time',      callback_data: 'exp_dl_all'          }],
    ]}}
  );
}

// ─── Expense Export (CSV + receipts ZIP) for a specific period ───────────────
async function downloadExpenses(chatId, userId, period = 'all') {
  try {
    const result = await buildExpenseZip(userId, period);

    if (!result) {
      return send(chatId,
        `💸 No expenses recorded yet.\n\nStart by logging an expense or sending a 📸 receipt photo.`,
        { reply_markup: { inline_keyboard: [[{ text: '📋 All Expenses', callback_data: 'nav_expenses' }]] }}
      );
    }

    if (result.empty) {
      return send(chatId,
        `📭 No expenses found for *${PERIOD_NAMES[period] || period}*.\n\nTry a different period or choose All Time.`,
        { reply_markup: { inline_keyboard: [
          [{ text: '📥 Pick Different Period', callback_data: 'nav_export_expenses' }],
          [{ text: '📋 All Expenses',          callback_data: 'nav_expenses'        }],
        ]}}
      );
    }

    const { zipPath, count, total, receiptCount: rc, currency, periodName } = result;

    await send(chatId, `⏳ _Preparing ${count} expense${count !== 1 ? 's' : ''}${rc > 0 ? ` + ${rc} receipt${rc > 1 ? 's' : ''}` : ''}…_`);

    let caption = `📦 *Expense Export — ${periodName}*\n`;
    caption += `━━━━━━━━━━━━━━━━━━━━━━━━\n`;
    caption += `💸 ${count} expense${count !== 1 ? 's' : ''}  ·  ${formatAmount(total, currency)}\n`;
    if (rc > 0) caption += `📸 ${rc} receipt file${rc !== 1 ? 's' : ''} included\n`;
    caption += `\n*How to use with your accountant:*\n`;
    caption += `1️⃣ Open \`expenses.csv\` in Excel or Numbers\n`;
    caption += `2️⃣ Fill the *Tax Amount* column with input VAT/GST you can reclaim\n`;
    caption += `3️⃣ Mark *Deductible* as NO for any personal items\n`;
    if (rc > 0) caption += `4️⃣ The \`receipts/\` folder matches each row by *Receipt File* column\n`;
    caption += `\n_Each row has a unique Expense ID for audit trail._`;

    await bot.sendDocument(chatId, zipPath, { caption, parse_mode: 'Markdown',
      reply_markup: { inline_keyboard: [
        [{ text: '📥 Download Another Period', callback_data: 'nav_export_expenses' }],
        [{ text: '📋 All Expenses',            callback_data: 'nav_expenses'        }],
      ]}
    });
    try { fs.unlinkSync(zipPath); } catch (_) {}
  } catch (err) {
    console.error('Expense export error:', err.message);
    send(chatId, '⚠️ Error creating expense export. Please try again.');
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
      [{ text: '❌ Cancel - Keep My Data',  callback_data: 'deletedata_cancel'  }],
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
    await send(chatId, `🎤 _"${transcribedText}"_\n\n⚡ Building your invoice...`);
    await handleTextMessage(chatId, userId, transcribedText, firstName);

  } catch (err) {
    console.error('Voice error:', err.message);
    // Use the user's most recent invoice as the example, or a generic one
    const lastInv    = (invoiceHistory[userId] || []).slice(-1)[0];
    const voiceExample = lastInv
      ? `_"${lastInv.service || lastInv.line_items?.[0]?.description || 'Services'} for ${lastInv.customer_name}, ${formatAmount(lastInv.total, lastInv.currency)}"_`
      : `_"Consulting for Ahmed, 3,000"_`;
    send(chatId,
      `⚠️ *Voice note couldn't be processed*\n\n` +
      `Please type your invoice instead:\n` +
      `${voiceExample}`
    );
  }
}

// ─── Text Message Router ──────────────────────────────────────────────────────
async function handleTextMessage(chatId, userId, text, firstName) {
  // ── Persistent keyboard button intercepts (exact match) ─────────────────────
  if (text === '📄 New Invoice') return showInvoicePrompt(chatId, userId);
  if (text === '💸 Log Expense') return showExpensePrompt(chatId, userId);

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

  // Expense logging — broad keyword shortcut (mirrors quickClassify in core.js)
  // Catches keywords BEFORE the AI classifier sees the text, preventing misclassification
  const expenseKeywords = /\b(spent|expense[d]?|paid\s+for|cost[s]?|bought|purchase[d]?|rent|petrol|fuel|gas|grocery|groceries|food|lunch|dinner|breakfast|coffee|meal|transport|taxi|uber|parking|toll|subscription|supplies|stationery|salary|wage[s]?|payroll|utility|utilities|electric|electricity|internet|insurance|maintenance|repair[s]?|travel|hotel|flight|airfare|marketing|advertising)\b/i;
  if (expenseKeywords.test(lower) && /\d/.test(lower)) {
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
  // Only route to list views for explicit navigation requests - NOT on any text
  // containing "invoice"/"bill" alone (that breaks multilingual voice/text input)
  if (/\b(my\s+invoices?|show\s+invoices?|list\s+invoices?|invoice\s+(list|history|overview)|see\s+invoices?)\b/i.test(lower)) return showInvoices(chatId, userId);
  if (/\b(my\s+clients?|show\s+clients?|list\s+clients?|my\s+customers?|show\s+customers?)\b/i.test(lower)) return showCustomers(chatId, userId);
  if (/\b(profile|settings?)\b/i.test(lower)) return showProfile(chatId, userId);

  // AI classification
  const intent = await classifyIntent(text);

  if (intent === 'invoice') {
    await handleInvoiceRequest(chatId, userId, sanitizeInput(text));
  } else if (intent === 'expense') {
    await handleExpenseEntry(chatId, userId, sanitizeInput(text));
  } else if (intent === 'greeting' || intent === 'help') {
    showWelcome(chatId, userId, firstName);
  } else if (intent === 'stats') {
    showPeriodSelector(chatId, userId, 'stats');
  } else if (intent === 'download') {
    showPeriodSelector(chatId, userId, 'download');
  } else {
    send(chatId,
      `❓ Not sure what you mean.\n\n` +
      `📄 To invoice: _"Website for Ahmed, 5000"_\n` +
      `💸 To log expense: _"Spent 300 on software"_\n\n` +
      `Or tap the quick-action buttons below.`
    );
  }
}

async function handleCommandState(chatId, userId, text) {
  const state = commandState[userId];
  if (!state) return;
  const lower = text.toLowerCase().trim();

  // ── Context routing from prompt screens ─────────────────────────────────────
  // After the expense/invoice prompt, ANY text the user types goes to the right handler.
  // No magic trigger words needed — the prompt established the intent.
  if (state.type === 'expect_expense') {
    delete commandState[userId];
    return handleExpenseEntry(chatId, userId, text);
  }
  if (state.type === 'expect_invoice') {
    delete commandState[userId];
    return handleInvoiceRequest(chatId, userId, text);
  }

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
      preview += `│ ${li.description} - *${formatAmount(li.amount, profile.currency)}*\n`;
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
      `📌 *Template Saved: "${templateName}"*\n\nUse it anytime with /templates - one tap invoicing! ⚡`,
      { reply_markup: { inline_keyboard: [[{ text: '📌 View Templates', callback_data: 'nav_templates' }]] }}
    );
  }

  // ── Add service to catalogue ─────────────────────────────────────────────────
  if (state.type === 'svc_add') {
    const profile = companyProfiles[userId];
    // Expect: "Service Name · 500" or "Service Name for 500"
    const match = text.match(/^(.+?)(?:\s*[·\-–for]+\s*|\s+)(\d+(?:\.\d+)?)\s*$/i);
    if (!match) {
      return send(chatId, '⚠️ Format: _"Service Name · Price"_\nExample: _"Website Design · 5000"_');
    }
    const [, name, price] = match;
    delete commandState[userId];
    const res = addService(userId, { name: name.trim(), defaultPrice: parseFloat(price), currency: profile.currency });
    return send(chatId,
      `✅ *Service Saved!*\n\n📦 *${name.trim()}*\n💰 ${formatAmount(parseFloat(price), profile.currency)}\n\n_Use it next time you create an invoice!_`,
      { reply_markup: { inline_keyboard: [[{ text: '📦 Services', callback_data: 'nav_services' }, { text: '🏠 Home', callback_data: 'nav_home' }]] }}
    );
  }

  // ── Partial payment amount ───────────────────────────────────────────────────
  if (state.type === 'partial_payment') {
    const amount = parseFloat(text.replace(/[^0-9.]/g, ''));
    if (isNaN(amount) || amount <= 0) {
      return send(chatId, '⚠️ Please enter a valid amount, e.g. `500`.',
        { reply_markup: { inline_keyboard: [[{ text: '❌ Cancel', callback_data: 'nav_invoices' }]] }}
      );
    }
    const result = recordPartialPayment(userId, state.invoiceId, amount, '');
    if (result.error) return send(chatId, `⚠️ ${result.error}`);
    delete commandState[userId];
    const currency = companyProfiles[userId]?.currency || 'AED';
    return send(chatId,
      `💰 *Partial Payment Recorded!*\n\n` +
      `Invoice: \`${state.invoiceId}\`\n` +
      `Paid: *${formatAmount(amount, currency)}*\n` +
      `Remaining: *${formatAmount(result.remaining, currency)}*\n` +
      `Status: ${result.status === 'paid' ? '✅ Fully Paid!' : '💛 Partial'}`,
      { reply_markup: { inline_keyboard: [[{ text: '📋 Invoices', callback_data: 'nav_invoices' }, { text: '🏠 Home', callback_data: 'nav_home' }]] }}
    );
  }

  // ── WA send - phone number input ────────────────────────────────────────────
  if (state.type === 'wa_send_phone') {
    const phone = text.replace(/[^+\d]/g, '');
    if (phone.length < 8) {
      return send(chatId, '⚠️ Please enter a valid phone number with country code, e.g. +971501234567');
    }
    saveClientWhatsApp(userId, state.customerName, phone);
    delete commandState[userId];
    await send(chatId, `✅ Saved ${state.customerName}'s number as ${phone}\n\n📱 Sending invoice now...`);
    return handleWaSendInvoice(chatId, userId, state.invoiceId);
  }

  // ── Email send - address input (CLIENT email) ────────────────────────────
  if (state.type === 'email_send_address') {
    const email = text.trim().toLowerCase();
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
      return send(chatId,
        `⚠️ That doesn't look like a valid email address.\n\nPlease enter a valid address, e.g. _ahmed@company.com_`
      );
    }
    saveClientEmail(userId, state.customerName, email);
    delete commandState[userId];
    await send(chatId, `✅ Saved ${state.customerName}'s email as ${email}\n\n📧 Sending invoice now...`);
    return handleEmailSendInvoice(chatId, userId, state.invoiceId);
  }

  // ── Email send - owner's own email (captured before first email send) ────
  if (state.type === 'owner_email_then_send') {
    const email = text.trim().toLowerCase();
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
      return send(chatId,
        `⚠️ That doesn't look like a valid email address.\n\nPlease enter yours, e.g. _you@yourbusiness.com_`,
        { reply_markup: { force_reply: true, selective: true,
            input_field_placeholder: 'e.g. you@yourbusiness.com' }}
      );
    }
    if (!companyProfiles[userId]) companyProfiles[userId] = {};
    companyProfiles[userId].owner_email = email;
    saveData();
    delete commandState[userId];
    await send(chatId, `✅ Saved — you'll be CC'd at *${email}* on every invoice email.\n\n📧 Now sending to ${state.customerName}...`);
    return handleEmailSendInvoice(chatId, userId, state.invoiceId);
  }

  // ── Credit note - invoice ID input ──────────────────────────────────────────
  if (state.type === 'credit_invoice_id') {
    const invoiceId = text.trim().toUpperCase();
    const inv = (invoiceHistory[userId] || []).find(i => i.invoice_id === invoiceId);
    if (!inv) return send(chatId, `⚠️ Invoice \`${invoiceId}\` not found. Check the ID and try again.`);
    commandState[userId] = { type: 'credit_amount', invoiceId, customerName: inv.customer_name, total: inv.total, currency: inv.currency };
    const currency = companyProfiles[userId]?.currency || inv.currency || 'AED';
    return send(chatId, `💰 How much is the credit for?\n\nInvoice total: *${formatAmount(inv.total, currency)}*\n_(Enter the credit amount, e.g. 500)_`);
  }

  if (state.type === 'credit_amount') {
    const amount = parseFloat(text.replace(/[^0-9.]/g, ''));
    if (isNaN(amount) || amount <= 0) return send(chatId, '⚠️ Please enter a valid amount.');
    commandState[userId] = { ...state, type: 'credit_reason', amount };
    return send(chatId, '📝 What is the reason for this credit?\n_(e.g. "Duplicate charge", "Service not rendered", "Discount applied")_');
  }

  if (state.type === 'credit_reason') {
    const reason = sanitizeInput(text);
    if (!reason) return send(chatId, '⚠️ Please provide a reason.');
    delete commandState[userId];
    await send(chatId, '🔴 _Generating credit note PDF..._');
    try {
      const result = await createCreditNote(userId, state.invoiceId, state.amount, reason);
      if (result.error) return send(chatId, `⚠️ ${result.error}`);
      const currency = companyProfiles[userId]?.currency || state.currency || 'AED';
      await bot.sendDocument(chatId, result.pdfPath, {
        caption: `🔴 *Credit Note ${result.creditId}*\n\nAmount: *${formatAmount(result.amount, currency)}*\nRef: \`${state.invoiceId}\``,
        parse_mode: 'Markdown',
        reply_markup: { inline_keyboard: [[{ text: '🏠 Home', callback_data: 'nav_home' }]] }
      });
      try { fs.unlinkSync(result.pdfPath); } catch (_) {}
    } catch (err) {
      console.error('Credit note error:', err.message);
      send(chatId, '⚠️ Error generating credit note. Please try again.');
    }
    return;
  }

  // ── Branding thank-you message ───────────────────────────────────────────────
  if (state.type === 'brand_thankyou') {
    const msg = sanitizeInput(text).slice(0, 120);
    saveBranding(userId, { thankYouMessage: msg });
    delete commandState[userId];
    return send(chatId, `✅ Thank-you message saved!\n\n_"${msg}"_\n\nThis will appear on all future invoices.`,
      { reply_markup: { inline_keyboard: [[{ text: '🎨 Branding', callback_data: 'cmd_branding' }]] }}
    );
  }

  if (state.type === 'brand_footer') {
    const note = sanitizeInput(text).slice(0, 80);
    saveBranding(userId, { footerNote: note });
    delete commandState[userId];
    return send(chatId, `✅ Footer note saved!\n\n_"${note}"_\n\nThis will appear in the invoice footer.`,
      { reply_markup: { inline_keyboard: [[{ text: '🎨 Branding', callback_data: 'cmd_branding' }]] }}
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
    // User typed something unrelated to a period — clear stuck state and
    // route the message normally so invoice/expense creation works.
    delete commandState[userId];
    await handleTextMessage(chatId, userId, text, '');
  }
}

// ─── Invoice Flow ─────────────────────────────────────────────────────────────
async function handleInvoiceRequest(chatId, userId, text) {
  if (!companyProfiles[userId]) {
    return send(chatId, '⚠️ Please set up your profile first with /setup before creating invoices.');
  }

  await send(chatId, '⚡ _Got it - extracting invoice details..._');

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

  let preview = `📋 *Invoice Preview*\n\n`;
  preview += `🏢 ${profile.company_name}  →  👤 *${data.customer_name}*\n`;
  if (data.address && data.address !== 'null' && data.address?.trim()) {
    preview += `📍 ${data.address}\n`;
  }
  preview += `\n`;

  data.line_items.forEach(item => {
    const amtStr = formatAmount(item.amount, profile.currency);
    preview += `• ${item.description}\n`;
    preview += `  *${amtStr}*\n`;
  });

  preview += `\n`;
  if (data.line_items.length > 1) {
    preview += `Subtotal: ${formatAmount(subtotal, profile.currency)}\n`;
  }
  if (tc.enabled && tax > 0) {
    preview += `${tc.type} ${tc.rate}%: ${formatAmount(tax, profile.currency)}\n`;
  }
  preview += `💰 *Total: ${formatAmount(total, profile.currency)}*\n\n`;
  preview += `${curr.flag || ''} ${profile.currency}  ·  ${new Date().toLocaleDateString('en-GB')}\n`;
  preview += `_Tap Generate to create your PDF ↓_`;

  await send(chatId, preview, {
    reply_markup: { inline_keyboard: [
      [{ text: '✅ Generate PDF',  callback_data: 'confirm_invoice' }],
      [{ text: '✏️ Edit Details',  callback_data: 'retry_invoice'  }]
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
      `✅ *Invoice Created*  \`${result.invoiceId}\`\n` +
      `👤 *${result.customer}*  ·  💰 *${formatAmount(result.total, result.currency)}*`;
    if (result.paymentUrl) {
      caption += `\n\n💳 *Payment link ready - share with client:*\n${result.paymentUrl}`;
    } else {
      caption += `\n\n_Forward the PDF above to your client._`;
    }

    // Build send buttons — personalised labels if client details already saved
    const clientPhone = getClientWhatsApp(userId, result.customer);
    const clientEmail = getClientEmail(userId, result.customer);
    const firstName   = result.customer.split(' ')[0];
    const waLabel     = clientPhone ? `📱 Send to ${firstName}` : `📱 Send to Client's WhatsApp`;
    const emailLabel  = clientEmail ? `📧 Email to ${firstName}` : `📧 Send to Client's Email`;

    await bot.sendDocument(chatId, result.pdfPath, { caption, parse_mode: 'Markdown',
      reply_markup: { inline_keyboard: [
        [{ text: '✅ Mark as Paid',     callback_data: `paid_${result.invoiceId}`            }],
        [{ text: waLabel,              callback_data: `wa_send_${result.invoiceId}`          }],
        [{ text: emailLabel,           callback_data: `email_send_${result.invoiceId}`       }],
        [
          { text: '💾 Save Template',  callback_data: 'save_template'                       },
          { text: '🔄 Make Recurring', callback_data: `recurring_setup_${result.invoiceId}` },
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

  let preview = `⚡ *Quick Re-Invoice*\n\n`;
  preview += `👤 *${customerName}*  _(last invoiced ${lastInv.date})_\n\n`;
  preview += `• ${lastInv.service || 'Previous service'}\n`;
  preview += `  *${formatAmount(lastInv.total, lastInv.currency || profile.currency)}*\n\n`;
  preview += `${curr.flag || ''} ${profile.currency}  ·  ${new Date().toLocaleDateString('en-GB')}\n`;
  preview += `_Same details as last time. Tap Generate or type a new amount._`;

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
      [{ text: '✅ Generate PDF',    callback_data: 'confirm_invoice' }],
      [{ text: '✏️ Edit Amount',     callback_data: 'retry_invoice'   }],
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

  let msg = `⏱ *Invoice Aging*\n\n`;

  if (report.count === 0) {
    msg += `✅ *All invoices are paid!*\n\nGreat work - nothing outstanding. 🎉`;
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
        msg += `  • \`${inv.invoice_id}\` - ${inv.customer_name} - *${formatAmount(inv.total, inv.currency || currency)}* (${inv.daysOld}d)\n`;
      });
      if (bucket.invoices.length > 3) msg += `  _+${bucket.invoices.length - 3} more_\n`;
      msg += '\n';
    }

    if (report.buckets.days90.invoices.length > 0) {
      msg += `🔴 *Action Required:* ${report.buckets.days90.invoices.length} invoice${report.buckets.days90.invoices.length !== 1 ? 's' : ''} over 90 days - consider escalating collection.\n`;
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

  let msg = `🎯 *Monthly Revenue Goal*\n\n`;

  if (goal) {
    const pct = Math.min(100, Math.round((monthStats.total / goal.monthly) * 100));
    const bar = asciiBar(monthStats.total, goal.monthly, 12);
    if (pct >= 100) {
      msg += `🏆 *Goal crushed!* You hit 100%!\n\n`;
      msg += `${bar} ${pct}%\n`;
      msg += `*${formatAmount(monthStats.total, currency)}* earned  ·  target was ${formatAmount(goal.monthly, currency)}\n\n`;
      msg += `_Set a bigger goal for next month?_`;
    } else {
      const remaining = goal.monthly - monthStats.total;
      msg += `${bar} *${pct}%*\n`;
      msg += `*${formatAmount(monthStats.total, currency)}* of ${formatAmount(goal.monthly, currency)}\n`;
      msg += `_${formatAmount(remaining, currency)} to go this month_\n\n`;
      msg += `_Update target (type new amount, e.g. \`10000\`):_`;
    }
  } else {
    msg += `No goal set yet.\n\n_What's your monthly revenue target?_\n_Type an amount, e.g. \`10000\`_`;
  }

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
      `📄 *Client Statement - ${customerName}*\n\n` +
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

  let msg = `📌 *Templates*\n\n`;

  if (templates.length === 0) {
    msg += `No templates yet.\n\n`;
    msg += `After creating an invoice, tap *💾 Save Template* to save it for one-tap reuse.`;
    return send(chatId, msg, { reply_markup: { inline_keyboard: [[{ text: '🏠 Home', callback_data: 'nav_home' }]] }});
  }

  const profile = companyProfiles[userId];
  const currency = profile?.currency || 'AED';

  msg += `${templates.length}/10 templates:\n\n`;
  templates.forEach((t, i) => {
    const total = (t.line_items || []).reduce((s, li) => s + (parseFloat(li.amount) || 0), 0);
    msg += `${i + 1}. *${t.name}*  -  ${formatAmount(total, currency)}\n`;
  });
  msg += `\n`;

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

  let msg = `📌 *${template.name}*\n\n`;
  (template.line_items || []).forEach(li => {
    msg += `• ${li.description}  -  ${formatAmount(li.amount, profile.currency)}\n`;
  });
  if (tc.enabled) msg += `${tc.type} ${tc.rate}%: ${formatAmount(tax, profile.currency)}\n`;
  msg += `💰 *Total: ${formatAmount(total, profile.currency)}*\n\n`;
  msg += `_Who is this invoice for? (Type customer name)_`;

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

// ─── Receipt Photo Handler ─────────────────────────────────────────────────────
async function handleReceiptPhoto(chatId, userId, photos) {
  if (!companyProfiles[userId]) {
    return send(chatId, '⚠️ Please set up your profile first with /setup.');
  }
  await send(chatId, '📸 _Scanning receipt..._');
  let receiptPath = null;
  try {
    // Use the highest-resolution version of the photo
    const fileId   = photos[photos.length - 1].file_id;
    const fileInfo = await bot.getFile(fileId);
    const fileUrl  = `https://api.telegram.org/file/bot${TELEGRAM_TOKEN}/${fileInfo.file_path}`;
    const imgRes   = await axios.get(fileUrl, { responseType: 'arraybuffer', timeout: 20000 });
    const imgBuf   = Buffer.from(imgRes.data);

    // Persist the image before AI processing
    const receiptFilename = `receipt_${userId}_${Date.now()}.jpg`;
    receiptPath = path.join(RECEIPTS_DIR, receiptFilename);
    fs.writeFileSync(receiptPath, imgBuf);

    // Extract expense data using Claude vision
    const data     = await extractExpenseFromImage(imgBuf);
    const profile  = companyProfiles[userId];
    const currency = profile.currency;

    if (!data.amount || parseFloat(data.amount) <= 0) {
      try { fs.unlinkSync(receiptPath); } catch (_) {}
      return send(chatId,
        '⚠️ Couldn\'t read a total amount from this receipt.\n\n' +
        'Try typing the expense instead:\n_"Spent 500 on petrol"_'
      );
    }

    // Store state including saved receipt path
    commandState[userId] = {
      type:        'receipt_confirm',
      expenseData: { ...data, receipt_path: receiptPath },
    };

    const merchantLine = data.merchant ? `🏪 *${data.merchant}*\n` : '';
    const dateLine     = data.date     ? `📅 ${data.date}\n`       : '';
    const msg =
      `📸 *Receipt Scanned*\n` +
      `━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
      `${merchantLine}` +
      `📝 ${data.description}\n` +
      `🏷 Category: *${data.category}*\n` +
      `💰 *${formatAmount(data.amount, currency)}*\n` +
      `${dateLine}\n` +
      `_Looks right? Image saved for tax records._`;

    await send(chatId, msg, { reply_markup: { inline_keyboard: [
      [{ text: '✅ Log Expense + Save Receipt', callback_data: 'rcpt_confirm' }],
      [{ text: '❌ Discard',                    callback_data: 'rcpt_cancel'  }],
    ]}});
  } catch (err) {
    console.error('Receipt scan error:', err.message);
    if (receiptPath) { try { fs.unlinkSync(receiptPath); } catch (_) {} }
    send(chatId,
      '⚠️ Couldn\'t read the receipt. Try typing the expense instead:\n' +
      '_"Spent 300 on coffee"_\n_"Paid 1200 for software license"_'
    );
  }
}

// ─── Receipt Document Handler (PDFs + images sent as files) ───────────────────
async function handleReceiptDocument(chatId, userId, doc) {
  if (!companyProfiles[userId]) {
    return send(chatId, '⚠️ Please set up your profile first with /setup.');
  }

  const mime      = doc.mime_type || '';
  const isPDF     = mime === 'application/pdf';
  const isImage   = mime.startsWith('image/');

  if (!isPDF && !isImage) {
    return send(chatId,
      '⚠️ I can scan *PDF documents* and *images* (JPEG, PNG).\n\n' +
      'Send a flight ticket, hotel booking, invoice, or any receipt PDF and I\'ll auto-log it as an expense.'
    );
  }

  // Enforce a 20 MB file size cap (Telegram Bot API limit)
  if (doc.file_size && doc.file_size > 20 * 1024 * 1024) {
    return send(chatId, '⚠️ File is too large (max 20 MB). Try a smaller version.');
  }

  await send(chatId, isPDF ? '📄 _Reading document..._' : '📸 _Scanning image..._');

  let receiptPath = null;
  try {
    const fileInfo = await bot.getFile(doc.file_id);
    const fileUrl  = `https://api.telegram.org/file/bot${TELEGRAM_TOKEN}/${fileInfo.file_path}`;
    const fileRes  = await axios.get(fileUrl, { responseType: 'arraybuffer', timeout: 30000 });
    const fileBuf  = Buffer.from(fileRes.data);

    // Determine file extension for storage
    const extMap  = { 'application/pdf': '.pdf', 'image/jpeg': '.jpg', 'image/png': '.png', 'image/webp': '.webp' };
    const ext     = extMap[mime] || (isPDF ? '.pdf' : '.jpg');
    const fname   = `receipt_${userId}_${Date.now()}${ext}`;
    receiptPath   = path.join(RECEIPTS_DIR, fname);
    fs.writeFileSync(receiptPath, fileBuf);

    // Extract using appropriate method
    const data     = isPDF ? await extractExpenseFromPDF(fileBuf) : await extractExpenseFromImage(fileBuf, mime);
    const profile  = companyProfiles[userId];
    const currency = profile.currency;

    if (!data.amount || parseFloat(data.amount) <= 0) {
      try { fs.unlinkSync(receiptPath); } catch (_) {}
      return send(chatId,
        '⚠️ Couldn\'t find a total amount in this document.\n\n' +
        'Try typing the expense instead:\n_"Flight to Dubai 850"_'
      );
    }

    commandState[userId] = {
      type:        'receipt_confirm',
      expenseData: { ...data, receipt_path: receiptPath },
    };

    const typeLabel    = isPDF ? '📄 *Document Scanned*' : '📸 *Image Scanned*';
    const merchantLine = data.merchant ? `🏪 *${data.merchant}*\n` : '';
    const dateLine     = data.date     ? `📅 ${data.date}\n`       : '';
    const msg =
      `${typeLabel}\n` +
      `━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
      `${merchantLine}` +
      `📝 ${data.description}\n` +
      `🏷 Category: *${data.category}*\n` +
      `💰 *${formatAmount(data.amount, currency)}*\n` +
      `${dateLine}\n` +
      `_Looks right? File saved for tax records._`;

    await send(chatId, msg, { reply_markup: { inline_keyboard: [
      [{ text: '✅ Log Expense + Save File', callback_data: 'rcpt_confirm' }],
      [{ text: '❌ Discard',                 callback_data: 'rcpt_cancel'  }],
    ]}});
  } catch (err) {
    console.error('Document scan error:', err.message);
    if (receiptPath) { try { fs.unlinkSync(receiptPath); } catch (_) {} }
    send(chatId,
      '⚠️ Couldn\'t read this document. Try typing the expense instead:\n' +
      '_"Flight to London 1200"_\n_"Hotel Marriott 600"_'
    );
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
      `*Log an expense:*\n` +
      `- Type: _"Spent 500 on petrol"_\n` +
      `- Send a 📸 photo of any receipt to auto-scan it\n` +
      `- Send a 📄 PDF (flight ticket, hotel booking, invoice) to auto-log it`,
      { reply_markup: { inline_keyboard: [[{ text: '🏠 Home', callback_data: 'nav_home' }]] }}
    );
  }

  const SHOW_LIMIT = 20;
  const recent     = expenses.slice(-SHOW_LIMIT).reverse();
  const thisMonth  = getExpenses(userId, 'this_month');
  const monthTotal = thisMonth.reduce((s, e) => s + (parseFloat(e.amount) || 0), 0);
  const allTotal   = expenses.reduce((s, e) => s + (parseFloat(e.amount) || 0), 0);
  const receiptCount = expenses.filter(e => e.receipt_path).length;

  let msg = `💸 *Expense Tracker*\n`;
  msg += `━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;
  msg += `📅 This Month: *${formatAmount(monthTotal, currency)}*\n`;
  msg += `📊 All Time: ${formatAmount(allTotal, currency)}  ·  ${expenses.length} expense${expenses.length !== 1 ? 's' : ''}\n`;
  if (receiptCount > 0) {
    msg += `📸 ${receiptCount} receipt${receiptCount > 1 ? 's' : ''} saved\n`;
  }
  msg += `\n*Recent Expenses:*\n\n`;

  const catEmoji = { Travel: '✈️', Software: '💻', Office: '🏢', Marketing: '📣', Subcontractors: '👷', Equipment: '🔧', Other: '📦' };
  recent.forEach(exp => {
    const icon         = catEmoji[exp.category] || '📦';
    const receiptBadge = exp.receipt_path ? ' 📸' : '';
    const merchantStr  = exp.merchant ? `  🏪 ${exp.merchant}  ·` : '';
    msg += `${icon} *${formatAmount(exp.amount, exp.currency || currency)}* - ${exp.description}${receiptBadge}\n`;
    msg += `  ${merchantStr} 🏷 ${exp.category}  ·  📅 ${exp.date}\n\n`;
  });

  if (expenses.length > SHOW_LIMIT) msg += `_+${expenses.length - SHOW_LIMIT} older expenses · export for full list_\n`;
  msg += `\n_Send a 📸 photo or 📄 PDF (tickets, invoices) to auto-scan._`;

  const exportLabel = receiptCount > 0
    ? `📥 Export CSV + ${receiptCount} Receipt${receiptCount > 1 ? 's' : ''}`
    : `📥 Export Expenses CSV`;

  await send(chatId, msg, { reply_markup: { inline_keyboard: [
    [{ text: exportLabel, callback_data: 'nav_export_expenses' }],
    [
      { text: '📈 P&L Report', callback_data: 'nav_profit' },
      { text: '🏠 Home',       callback_data: 'nav_home'   },
    ],
  ]}});
}

async function showProfitLoss(chatId, userId, period) {
  const profile = companyProfiles[userId];
  if (!profile) return send(chatId, '⚠️ Please set up your profile first.');

  const pl       = calculateProfitLoss(userId, period);
  const currency = profile.currency;
  const isProfit = pl.profit >= 0;

  let msg = `📈 *Profit & Loss - ${PERIOD_NAMES[period] || period}*\n`;
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
    `${insight}`;

  await send(chatId, msg, { reply_markup: { inline_keyboard: [
    [
      { text: '📊 Full Stats',   callback_data: `stats_${period}` },
      { text: '🎯 Set Goal',     callback_data: 'nav_goal'        },
    ],
    [{ text: '🏠 Home', callback_data: 'nav_home' }]
  ]}});
}

// ═══════════════════════════════════════════════════════════════════════════════
// ─── v2.2 NEW FEATURE HANDLERS ────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════════════════

// ─── Services Catalogue ───────────────────────────────────────────────────────
async function showServices(chatId, userId) {
  const profile = companyProfiles[userId];
  if (!profile) return send(chatId, '⚠️ Please set up your profile first with /setup.');

  const services = getServices(userId);
  let msg = `📦 *Services & Products Catalogue*\n\n`;

  if (services.length === 0) {
    msg += `No services saved yet.\n\n`;
    msg += `Add services to quickly fill invoices:\n`;
    msg += `_e.g. "Web Design · 5000 AED"_`;
  } else {
    msg += `${services.length} service${services.length !== 1 ? 's' : ''} saved:\n\n`;
    services.forEach((s, i) => {
      msg += `*${i + 1}.* ${s.name}`;
      if (s.defaultPrice > 0) msg += `  -  ${formatAmount(s.defaultPrice, profile.currency)}`;
      if (s.description) msg += `\n   _${s.description}_`;
      msg += `\n`;
    });
  }

  const keyboard = [];
  // Delete buttons for each service (max 5 shown)
  services.slice(0, 5).forEach(s => {
    keyboard.push([{ text: `🗑 Remove: ${s.name}`, callback_data: `svc_del_${s.id}` }]);
  });
  keyboard.push([{ text: '➕ Add Service',   callback_data: 'svc_add'    }]);
  keyboard.push([{ text: '🏠 Home',          callback_data: 'nav_home'   }]);

  await send(chatId, msg, { reply_markup: { inline_keyboard: keyboard } });
}

// ─── Quotes ───────────────────────────────────────────────────────────────────
async function showQuotes(chatId, userId) {
  const profile = companyProfiles[userId];
  if (!profile) return send(chatId, '⚠️ Please set up your profile first with /setup.');

  const quotes = getQuotes(userId);
  let msg = `📝 *Quotes*\n\n`;

  const STATUS_ICONS = { draft: '📝', sent: '📤', converted: '✅', declined: '❌' };

  if (quotes.length === 0) {
    msg += `No quotes yet.\n\nCreate a quote from the invoice preview - tap _"Save as Quote"_ instead of generating the PDF.`;
  } else {
    const recent = quotes.slice(0, 8);
    recent.forEach(q => {
      const icon = STATUS_ICONS[q.status] || '📝';
      msg += `${icon} *${q.customer_name}*  -  ${formatAmount(q.total, q.currency || profile.currency)}\n`;
      msg += `   \`${q.quote_id}\`  ·  ${q.date}  ·  _${q.status}_\n`;
    });
    if (quotes.length > 8) msg += `\n_+${quotes.length - 8} more_`;
  }

  // Convert buttons for draft/sent quotes
  const convertable = quotes.filter(q => q.status !== 'converted' && q.status !== 'declined').slice(0, 3);
  const keyboard = [];
  convertable.forEach(q => {
    keyboard.push([{ text: `🔄 Convert: ${q.quote_id} → Invoice`, callback_data: `quote_convert_${q.quote_id}` }]);
  });
  keyboard.push([{ text: '🏠 Home', callback_data: 'nav_home' }]);

  await send(chatId, msg, { reply_markup: { inline_keyboard: keyboard } });
}

// ─── Client Directory ──────────────────────────────────────────────────────────
async function showClients(chatId, userId) {
  const profile = companyProfiles[userId];
  if (!profile) return send(chatId, '⚠️ Please set up your profile first with /setup.');

  const clients = listClients(userId);
  let msg = `👤 *Client Directory*\n`;
  msg += `━━━━━━━━━━━━━━━━━━━━━━━━\n\n`;

  if (clients.length === 0) {
    msg += `No clients saved yet.\n\n`;
    msg += `Contact details are saved automatically the first time you send an invoice to a client via WhatsApp or Email.`;
  } else {
    msg += `${clients.length} client${clients.length !== 1 ? 's' : ''} saved\n\n`;
    clients.forEach(c => {
      msg += `👤 *${c.name}*\n`;
      if (c.whatsapp) msg += `   📱 \`${c.whatsapp}\`\n`;
      if (c.email)    msg += `   📧 \`${c.email}\`\n`;
      if (!c.whatsapp && !c.email) msg += `   _No contact details yet_\n`;
      msg += '\n';
    });
  }

  const keyboard = [];
  clients.slice(0, 5).forEach(c => {
    keyboard.push([{ text: `🗑 Remove: ${c.name}`, callback_data: `client_del_${encodeURIComponent(c.name)}` }]);
  });
  keyboard.push([{ text: '🏠 Home', callback_data: 'nav_home' }]);

  await send(chatId, msg, { reply_markup: { inline_keyboard: keyboard } });
}

// ─── Recurring Invoices ────────────────────────────────────────────────────────
async function showRecurring(chatId, userId) {
  const profile = companyProfiles[userId];
  if (!profile) return send(chatId, '⚠️ Please set up your profile first with /setup.');

  const recs = (recurringInvoices[userId] || []);
  const active = recs.filter(r => r.active);
  const paused = recs.filter(r => !r.active);

  let msg = `🔄 *Recurring Invoices*\n\n`;

  if (recs.length === 0) {
    msg += `No recurring invoices set up.\n\nAfter creating an invoice, tap _"Make Recurring"_ to auto-bill on a schedule.`;
  } else {
    if (active.length > 0) {
      msg += `✅ *Active (${active.length})*\n`;
      active.forEach(r => {
        msg += `• *${r.name}*  -  ${r.frequency}\n`;
        msg += `   Next: ${r.nextDue}\n`;
      });
      msg += `\n`;
    }
    if (paused.length > 0) {
      msg += `⏸ *Paused (${paused.length})*\n`;
      paused.forEach(r => {
        msg += `• ${r.name}  _(paused)_\n`;
      });
    }
  }

  const keyboard = [];
  recs.slice(0, 4).forEach(r => {
    const pauseLabel = r.active ? `⏸ Pause` : `▶ Resume`;
    keyboard.push([
      { text: `${pauseLabel}: ${r.name.substring(0, 20)}`, callback_data: `rec_toggle_${r.id}` },
      { text: '❌',                                          callback_data: `rec_del_${r.id}`    },
    ]);
  });
  keyboard.push([{ text: '🏠 Home', callback_data: 'nav_home' }]);

  await send(chatId, msg, { reply_markup: { inline_keyboard: keyboard } });
}

// ─── VAT / Tax Report ──────────────────────────────────────────────────────────
async function showVatReportSelector(chatId, userId) {
  const profile = companyProfiles[userId];
  if (!profile) return send(chatId, '⚠️ Please set up your profile first with /setup.');

  const year = new Date().getFullYear();
  await send(chatId,
    `📊 *VAT / Tax Report*\n\nSelect a quarter to generate your tax report PDF:`,
    { reply_markup: { inline_keyboard: [
      [
        { text: `Q1 Jan–Mar ${year}`, callback_data: `vat_1_${year}` },
        { text: `Q2 Apr–Jun ${year}`, callback_data: `vat_2_${year}` },
      ],
      [
        { text: `Q3 Jul–Sep ${year}`, callback_data: `vat_3_${year}` },
        { text: `Q4 Oct–Dec ${year}`, callback_data: `vat_4_${year}` },
      ],
      [
        { text: `Q1–Q4 ${year - 1}`, callback_data: `vat_1_${year - 1}` },
      ],
      [{ text: '🏠 Home', callback_data: 'nav_home' }]
    ]}}
  );
}

async function handleVatReport(chatId, userId, quarter, year) {
  await send(chatId, `📊 _Generating ${year} Q${quarter} Tax Report..._`);
  try {
    const pdfPath = await generateTaxReport(userId, parseInt(quarter), parseInt(year));
    if (!pdfPath) return send(chatId, '⚠️ No invoices with VAT found for that period.');
    await bot.sendDocument(chatId, pdfPath, {
      caption: `📊 *VAT Report - Q${quarter} ${year}*\n\nFor record-keeping and accountant submission.\n_Verify with your tax advisor before filing._`,
      parse_mode: 'Markdown',
      reply_markup: { inline_keyboard: [[{ text: '🏠 Home', callback_data: 'nav_home' }]] }
    });
    try { fs.unlinkSync(pdfPath); } catch (_) {}
  } catch (err) {
    console.error('VAT report error:', err.message);
    send(chatId, '⚠️ Error generating report. Please try again.');
  }
}

// ─── Cash Flow Forecast ────────────────────────────────────────────────────────
async function showCashFlowForecast(chatId, userId) {
  const profile = companyProfiles[userId];
  if (!profile) return send(chatId, '⚠️ Please set up your profile first with /setup.');

  await send(chatId, '🔮 _Analyzing your cash flow..._');

  try {
    const f = await generateCashFlowForecast(userId);
    if (!f) return send(chatId, '⚠️ No data yet. Create some invoices first!');

    const now = new Date();
    const monthName = now.toLocaleString('en', { month: 'long' });

    let msg = `🔮 *Cash Flow Forecast - ${monthName} ${now.getFullYear()}*\n\n`;
    msg += `📅 *Next 30 days:* ~${formatAmount(f.forecast30, f.currency)}\n`;
    msg += `📅 *Next 60 days:* ~${formatAmount(f.forecast60, f.currency)}\n`;
    msg += `📅 *Next 90 days:* ~${formatAmount(f.forecast90, f.currency)}\n\n`;

    msg += `📊 *Current Status*\n`;
    if (f.unpaidCount > 0) msg += `⏳ Outstanding: ${formatAmount(f.unpaidTotal, f.currency)} (${f.unpaidCount} invoices)\n`;
    if (f.overdueCount > 0) msg += `🔴 At risk (60+ days): ${formatAmount(f.overdueRisk, f.currency)}\n`;
    if (f.recurringCount > 0) msg += `🔄 Active recurring: ${f.recurringCount}\n`;
    msg += `📈 Monthly avg (6m): ${formatAmount(f.monthlyAvg, f.currency)}\n`;

    if (f.aiInsight) {
      msg += `\n💡 *Insight*\n${f.aiInsight}\n`;
    }

    await send(chatId, msg, { reply_markup: { inline_keyboard: [
      [
        { text: '⏱ Aging',    callback_data: 'nav_aging'    },
        { text: '🔄 Recurring', callback_data: 'nav_recurring' },
      ],
      [{ text: '🏠 Home', callback_data: 'nav_home' }]
    ]}});
  } catch (err) {
    console.error('Forecast error:', err.message);
    send(chatId, '⚠️ Error generating forecast. Please try again.');
  }
}

// ─── Credit Notes ──────────────────────────────────────────────────────────────
async function showCreditNotes(chatId, userId) {
  const profile = companyProfiles[userId];
  if (!profile) return send(chatId, '⚠️ Please set up your profile first with /setup.');

  const credits = getCreditNotes(userId);
  let msg = `🔴 *Credit Notes*\n\n`;

  if (credits.length === 0) {
    msg += `No credit notes issued yet.\n\nTo issue a credit note, view an invoice and tap _"Issue Credit Note"_.`;
  } else {
    credits.slice(0, 8).forEach(c => {
      msg += `• *${c.credit_id}*  -  ${c.customer_name}\n`;
      msg += `  ${formatAmount(c.amount, c.currency)}  ·  ${c.date}  ·  Ref: \`${c.original_invoice_id}\`\n`;
      msg += `  _${c.reason}_\n`;
    });
  }

  // Issue new credit note - prompt for invoice ID
  const keyboard = [
    [{ text: '➕ Issue Credit Note', callback_data: 'credit_new' }],
    [{ text: '🏠 Home',             callback_data: 'nav_home'   }]
  ];
  await send(chatId, msg, { reply_markup: { inline_keyboard: keyboard } });
}

// ─── Custom Branding ───────────────────────────────────────────────────────────
async function showBrandingSettings(chatId, userId) {
  const profile = companyProfiles[userId];
  if (!profile) return send(chatId, '⚠️ Please set up your profile first with /setup.');

  const branding = getBranding(userId);
  const currentColor = Object.values(BRANDING_COLORS).find(c => c.hex === branding.accentColor) || BRANDING_COLORS.indigo;

  let msg = `🎨 *Invoice Branding*\n\n`;
  msg += `Current settings:\n`;
  msg += `• Color: *${currentColor.name}*\n`;
  msg += `• Thank-you: _${branding.thankYouMessage || 'Not set'}_\n`;
  msg += `• Footer note: _${branding.footerNote || 'Not set'}_\n\n`;
  msg += `_Changes apply to all future invoices._`;

  const colorButtons = Object.entries(BRANDING_COLORS).map(([key, c]) => ({
    text: `${c.name === currentColor.name ? '✓ ' : ''}${c.name}`,
    callback_data: `brand_color_${key}`
  }));

  await send(chatId, msg, { reply_markup: { inline_keyboard: [
    colorButtons.slice(0, 3),
    colorButtons.slice(3),
    [{ text: '💬 Set Thank-You Message', callback_data: 'brand_thankyou' }],
    [{ text: '📝 Set Footer Note',       callback_data: 'brand_footer'   }],
    [{ text: '🔄 Reset to Default',      callback_data: 'brand_reset'    }],
    [{ text: '🏠 Home',                  callback_data: 'nav_home'       }]
  ]}});
}

// ─── Partial Payment Handler ────────────────────────────────────────────────────
async function handlePartialPayment(chatId, userId, invoiceId, queryId) {
  if (queryId) {
    bot.answerCallbackQuery(queryId).catch(() => {});
  }
  const inv = (invoiceHistory[userId] || []).find(i => i.invoice_id === invoiceId);
  if (!inv) return send(chatId, '⚠️ Invoice not found.');

  const currency = companyProfiles[userId]?.currency || inv.currency || 'AED';
  const remaining = inv.remaining || inv.total;

  commandState[userId] = { type: 'partial_payment', invoiceId };
  await send(chatId,
    `💰 *Partial Payment*\n\n` +
    `Invoice: \`${invoiceId}\`\n` +
    `Customer: *${inv.customer_name}*\n` +
    `Remaining: *${formatAmount(remaining, currency)}*\n\n` +
    `How much did they pay? _(just type the amount, e.g. 500)_`
  );
}

// ─── WA Send Invoice to Client ─────────────────────────────────────────────────
async function handleWaSendInvoice(chatId, userId, invoiceId, queryId) {
  if (queryId) {
    bot.answerCallbackQuery(queryId).catch(() => {});
  }
  const inv = (invoiceHistory[userId] || []).find(i => i.invoice_id === invoiceId);
  if (!inv) return send(chatId, '⚠️ Invoice not found.');

  const WA_TOKEN   = process.env.WHATSAPP_TOKEN;
  const WA_PHONE   = process.env.WHATSAPP_PHONE_ID;

  if (!WA_TOKEN || !WA_PHONE) {
    return send(chatId,
      '⚠️ *WhatsApp sending not configured.*\n\nAdd `WHATSAPP_TOKEN` and `WHATSAPP_PHONE_ID` to your `.env` file on the VPS to enable direct client delivery.',
      { reply_markup: { inline_keyboard: [[{ text: '🏠 Home', callback_data: 'nav_home' }]] }}
    );
  }

  const clientPhone = getClientWhatsApp(userId, inv.customer_name);

  if (clientPhone) {
    await send(chatId, `📱 _Sending to ${inv.customer_name} at ${clientPhone}..._`);
    try {
      const profile = companyProfiles[userId];
      // Plain text only - no Markdown (WhatsApp API rejects asterisks/backticks in some regions)
      const waMsg = [
        `Hello ${inv.customer_name} 👋`,
        ``,
        `Invoice ${invoiceId} from ${profile?.company_name || 'InvoKash'}`,
        `Amount: ${formatAmount(inv.total, inv.currency || profile?.currency)}`,
        `Date: ${inv.date}`,
        inv.payment_link ? `\nPay online: ${inv.payment_link}` : '',
        ``,
        `Thank you for your business! 🙏`,
      ].join('\n');

      const resp = await axios.post(
        `https://graph.facebook.com/v19.0/${WA_PHONE}/messages`,
        {
          messaging_product: 'whatsapp',
          to: clientPhone.replace(/[^0-9]/g, ''),   // digits only, no +
          type: 'text',
          text: { body: waMsg },
        },
        {
          headers: { Authorization: `Bearer ${WA_TOKEN}`, 'Content-Type': 'application/json' },
          timeout: 12000,   // 12-second timeout - prevents hanging
        }
      );

      console.log(`WA send OK → ${clientPhone}:`, resp.data?.messages?.[0]?.id);

      await send(chatId,
        `✅ *Invoice sent to ${inv.customer_name}!*\n\n📱 ${clientPhone}\n_They received the invoice details` +
        `${inv.payment_link ? ' + payment link' : ''}._`,
        { reply_markup: { inline_keyboard: [
          [{ text: '📋 Invoices', callback_data: 'nav_invoices' }],
          [{ text: '🏠 Home',     callback_data: 'nav_home'     }],
        ]}}
      );
    } catch (err) {
      const detail = err.response?.data?.error?.message || err.message || 'Unknown error';
      console.error('WA send error:', detail);
      send(chatId,
        `⚠️ *Couldn't send to ${inv.customer_name}*\n\n_${detail}_\n\n` +
        `Check that:\n• The number format is correct (e.g. +971501234567)\n` +
        `• Your WhatsApp Business account is active\n• The recipient has WhatsApp`,
        { reply_markup: { inline_keyboard: [
          [{ text: '📱 Update Number', callback_data: `wa_send_${invoiceId}` }],
          [{ text: '🏠 Home',          callback_data: 'nav_home'             }],
        ]}}
      );
    }
  } else {
    // Ask for phone number
    commandState[userId] = { type: 'wa_send_phone', invoiceId, customerName: inv.customer_name };
    await send(chatId,
      `📱 *Send Invoice to Client*\n\n` +
      `What's ${inv.customer_name}'s WhatsApp number?\n` +
      `_(Include country code, e.g. +971501234567)_\n\n` +
      `_I'll save it for next time too!_`
    );
  }
}

// ─── Email Invoice to Client ──────────────────────────────────────────────────
async function handleEmailSendInvoice(chatId, userId, invoiceId, queryId) {
  if (queryId) bot.answerCallbackQuery(queryId).catch(() => {});

  const inv = (invoiceHistory[userId] || []).find(i => i.invoice_id === invoiceId);
  if (!inv) return send(chatId, '⚠️ Invoice not found.');

  const profile     = companyProfiles[userId];
  const clientEmail = getClientEmail(userId, inv.customer_name);

  // ── Step 1: collect YOUR email first if missing ────────────────────────────
  if (!profile.owner_email) {
    commandState[userId] = { type: 'owner_email_then_send', invoiceId, customerName: inv.customer_name };
    return send(chatId,
      `📧 *Send Invoice by Email*\n\n` +
      `First — what's *your* email address?\n` +
      `_(You'll receive a copy of every invoice you send)_`,
      { reply_markup: { force_reply: true, selective: true,
          input_field_placeholder: 'e.g. you@yourbusiness.com' }}
    );
  }

  // ── Step 2: collect CLIENT email if missing ────────────────────────────────
  if (!clientEmail) {
    commandState[userId] = { type: 'email_send_address', invoiceId, customerName: inv.customer_name };
    return send(chatId,
      `📧 *Send Invoice by Email*\n\n` +
      `What's *${inv.customer_name}'s* email address?\n\n` +
      `_I'll save it for next time!_`,
      { reply_markup: { force_reply: true, selective: true,
          input_field_placeholder: 'e.g. client@company.com' }}
    );
  }

  // ── Step 3: attempt send — SMTP check happens here, after emails collected ─
  if (!process.env.SMTP_HOST || !process.env.SMTP_USER || !process.env.SMTP_PASS) {
    return send(chatId,
      `📧 *Emails saved!*\n\n` +
      `• To: \`${clientEmail}\`\n` +
      `• CC: \`${profile.owner_email}\`\n\n` +
      `⚠️ Email delivery isn't activated yet on this server.\n` +
      `Add \`SMTP_HOST\`, \`SMTP_USER\`, and \`SMTP_PASS\` to the \`.env\` file and restart the bot.\n\n` +
      `_See \`.env.example\` for Gmail / Outlook setup instructions._`,
      { reply_markup: { inline_keyboard: [[{ text: '🏠 Home', callback_data: 'nav_home' }]] }}
    );
  }

  await send(chatId, `📧 _Sending to ${inv.customer_name} at ${clientEmail}…_`);
  try {
    await sendInvoiceEmail(clientEmail, inv.customer_name, profile, inv, inv.file_path);

    const ccLine = profile.owner_email ? `\n📬 CC'd to you at ${profile.owner_email}` : '';
    await send(chatId,
      `✅ *Invoice emailed to ${inv.customer_name}!*\n\n` +
      `📧 ${clientEmail}${ccLine}\n` +
      `_PDF attached${inv.payment_link ? ' + payment link included' : ''}._`,
      { reply_markup: { inline_keyboard: [
        [{ text: '📋 Invoices', callback_data: 'nav_invoices' }],
        [{ text: '🏠 Home',     callback_data: 'nav_home'     }],
      ]}}
    );
  } catch (err) {
    console.error('Email send error:', err.message);
    send(chatId,
      `⚠️ *Couldn't send the email*\n\n_${err.message}_\n\nCheck your SMTP settings and try again.`,
      { reply_markup: { inline_keyboard: [
        [{ text: '📧 Try Again', callback_data: `email_send_${invoiceId}` }],
        [{ text: '🏠 Home',      callback_data: 'nav_home'                }],
      ]}}
    );
  }
}

// ─── Recurring Setup Handler ────────────────────────────────────────────────────
async function handleRecurringSetup(chatId, userId, invoiceId) {
  const inv = (invoiceHistory[userId] || []).find(i => i.invoice_id === invoiceId);
  if (!inv) return send(chatId, '⚠️ Invoice not found.');

  commandState[userId] = { type: 'recurring_setup', invoiceId };
  await send(chatId,
    `🔄 *Make Recurring*\n\n` +
    `Invoice: *${inv.customer_name}* - ${formatAmount(inv.total, inv.currency)}\n\n` +
    `How often should this auto-generate?`,
    { reply_markup: { inline_keyboard: [
      [{ text: '📅 Weekly',    callback_data: `rec_freq_weekly_${invoiceId}`    }],
      [{ text: '📅 Monthly',   callback_data: `rec_freq_monthly_${invoiceId}`   }],
      [{ text: '📅 Quarterly', callback_data: `rec_freq_quarterly_${invoiceId}` }],
      [{ text: '❌ Cancel',    callback_data: 'nav_home'                        }],
    ]}}
  );
}

async function handleRecurringFrequency(chatId, userId, frequency, invoiceId) {
  const inv = (invoiceHistory[userId] || []).find(i => i.invoice_id === invoiceId);
  if (!inv) return send(chatId, '⚠️ Invoice not found.');

  const templateData = {
    customer_name: inv.customer_name, address: null,
    line_items: [{ description: inv.service || 'Services', amount: parseFloat(inv.total) - parseFloat(inv.tax_amount || 0) }],
  };

  const result = createRecurring(userId, templateData, frequency);
  delete commandState[userId];

  await send(chatId,
    `✅ *Recurring Invoice Set!*\n\n` +
    `*${inv.customer_name}*  -  ${formatAmount(inv.total, inv.currency)}\n` +
    `📅 Frequency: *${frequency.charAt(0).toUpperCase() + frequency.slice(1)}*\n` +
    `⏰ First auto-generation: *${result.recurring.nextDue}*\n\n` +
    `_I'll generate this invoice automatically and notify you each time._`,
    { reply_markup: { inline_keyboard: [
      [{ text: '🔄 Recurring', callback_data: 'nav_recurring' }],
      [{ text: '🏠 Home',     callback_data: 'nav_home'      }]
    ]}}
  );
}

// ─── Start Function ───────────────────────────────────────────────────────────
function startTelegramBot() {
  if (!TELEGRAM_TOKEN) {
    console.warn('⚠️  TELEGRAM_TOKEN not set - Telegram bot disabled.');
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
      return send(chatId, '⏱ You\'re sending too fast - please wait a moment.');
    }

    try {
      if (text.startsWith('/')) { await handleCommand(chatId, userId, text, firstName); return; }
      // Persistent keyboard shortcuts always escape any stuck state
      if (text === '📄 New Invoice') { delete commandState[userId]; return showInvoicePrompt(chatId, userId); }
      if (text === '💸 Log Expense') { delete commandState[userId]; return showExpensePrompt(chatId, userId); }
      if (msg.photo && onboardingState[userId]?.step === 'logo') { await handleLogoUpload(chatId, userId, msg.photo); return; }
      if (msg.photo && onboardingState[userId]) return; // ignore photos during other onboarding steps
      if (msg.photo && companyProfiles[userId]) { await handleReceiptPhoto(chatId, userId, msg.photo); return; }
      if (msg.document && onboardingState[userId]) return; // ignore documents during onboarding
      if (msg.document && companyProfiles[userId]) { await handleReceiptDocument(chatId, userId, msg.document); return; }
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
        const pending = pendingInvoices[userId];
        delete pendingInvoices[userId];

        const original = pending?.originalText || '';

        // ForceReply: Telegram focuses the reply input automatically.
        // input_field_placeholder (max 64 chars) shows their original text
        // as ghost hint text inside the input box - as close to a pre-filled
        // edit field as Telegram's platform allows.
        const placeholder = original.slice(0, 64);

        const body = original
          ? `✏️ *Edit Invoice*\n\nYou wrote:\n_"${original}"_\n\nType your corrected version below:`
          : `✏️ *Edit Invoice*\n\nType your corrected invoice description:`;

        bot.sendMessage(chatId, body, {
          parse_mode:   'Markdown',
          reply_markup: {
            force_reply:             true,
            selective:               true,
            input_field_placeholder: placeholder || 'e.g. Consulting for Ahmed, 3000',
          },
        }).catch(() => {});
      }
      else if (data === 'nav_home')           showWelcome(chatId, userId, firstName);
      else if (data === 'nav_new_invoice')   showInvoicePrompt(chatId, userId);
      else if (data === 'nav_log_expense')   showExpensePrompt(chatId, userId);
      else if (data === 'nav_invoices')      { delete commandState[userId]; showInvoices(chatId, userId); }
      else if (data === 'nav_stats')         showPeriodSelector(chatId, userId, 'stats');
      else if (data === 'nav_profile')       showProfile(chatId, userId);
      else if (data === 'nav_download')      showPeriodSelector(chatId, userId, 'download');
      else if (data === 'nav_customers')     showCustomers(chatId, userId); // "Clients" in UI
      else if (data === 'nav_aging')         showAgingDashboard(chatId, userId);
      else if (data === 'nav_goal')          showGoalSetter(chatId, userId);
      else if (data === 'nav_templates')     showTemplates(chatId, userId);
      else if (data === 'nav_expenses')          showExpenses(chatId, userId);
      else if (data === 'nav_export_expenses')   showExpenseDownloadPicker(chatId, userId);
      else if (data.startsWith('exp_dl_'))       downloadExpenses(chatId, userId, data.replace('exp_dl_', ''));
      else if (data === 'nav_profit')            showProfitLoss(chatId, userId, 'this_month');
      else if (data === 'nav_statement')     selectClientForStatement(chatId, userId);
      // v2.2 nav
      else if (data === 'nav_services')      showServices(chatId, userId);
      else if (data === 'nav_quotes')        showQuotes(chatId, userId);
      else if (data === 'nav_clients')       showClients(chatId, userId);
      else if (data === 'nav_recurring')     showRecurring(chatId, userId);
      else if (data === 'nav_forecast')      showCashFlowForecast(chatId, userId);
      else if (data === 'nav_credits')       showCreditNotes(chatId, userId);
      else if (data === 'cmd_branding')      showBrandingSettings(chatId, userId);
      else if (data === 'cmd_vat')           showVatReportSelector(chatId, userId);
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
            `✅ *Expense Logged*\n\n` +
            `📝 ${expense.description}\n` +
            `🏷 ${expense.category}  ·  💰 *${formatAmount(expense.amount, currency)}*`,
            { reply_markup: { inline_keyboard: [
              [{ text: '📋 All Expenses',       callback_data: 'nav_expenses'        }],
              [{ text: '📥 Download Expenses',  callback_data: 'nav_export_expenses' }],
            ]}}
          );
        }
      }
      else if (data === 'exp_cancel') {
        delete commandState[userId];
        send(chatId, '❌ Expense cancelled.');
      }
      else if (data === 'rcpt_confirm') {
        const state = commandState[userId];
        if (state?.type === 'receipt_confirm' && state.expenseData) {
          const expense  = logExpense(userId, state.expenseData);
          delete commandState[userId];
          const currency = companyProfiles[userId]?.currency || 'AED';
          const merchantNote = expense.merchant ? `  ·  🏪 ${expense.merchant}` : '';
          send(chatId,
            `✅ *Expense Logged*\n\n` +
            `📝 ${expense.description}${merchantNote}\n` +
            `🏷 ${expense.category}  ·  📅 ${expense.date}\n` +
            `💰 *${formatAmount(expense.amount, currency)}*\n` +
            `📸 Receipt saved`,
            { reply_markup: { inline_keyboard: [
              [{ text: '📋 All Expenses',       callback_data: 'nav_expenses'        }],
              [{ text: '📥 Download Expenses',  callback_data: 'nav_export_expenses' }],
            ]}}
          );
        }
      }
      else if (data === 'rcpt_cancel') {
        const state = commandState[userId];
        if (state?.type === 'receipt_confirm' && state.expenseData?.receipt_path) {
          try { fs.unlinkSync(state.expenseData.receipt_path); } catch (_) {}
        }
        delete commandState[userId];
        send(chatId, '❌ Receipt discarded.');
      }
      else if (data === 'save_template') {
        const lastInv = (invoiceHistory[userId] || []).slice(-1)[0];
        if (!lastInv) return send(chatId, '⚠️ No invoice to save as template.');
        commandState[userId] = { type: 'template_name', lastInv };
        send(chatId, `📌 *Save as Template*\n\nGive this template a name:\n_e.g. "Monthly Retainer", "Web Design", "Consulting"_`);
      }
      else if (data.startsWith('noop_'))      bot.answerCallbackQuery(query.id).catch(() => {});
      else if (data.startsWith('paid_'))     handleMarkPaid(chatId, userId, data.replace('paid_', ''), query.id);
      // v2.2 callbacks
      else if (data.startsWith('partial_'))  handlePartialPayment(chatId, userId, data.replace('partial_', ''), query.id);
      else if (data.startsWith('wa_send_'))    handleWaSendInvoice(chatId, userId, data.replace('wa_send_', ''), query.id);
      else if (data.startsWith('email_send_')) handleEmailSendInvoice(chatId, userId, data.replace('email_send_', ''), query.id);
      else if (data === 'save_as_quote') {
        const pending = pendingInvoices[userId];
        if (!pending) return send(chatId, '⚠️ No pending quote data. Try describing the invoice again.');
        await send(chatId, '📝 _Generating quote PDF..._');
        const result = await createQuote(userId, pending.data);
        delete pendingInvoices[userId];
        if (result.error) return send(chatId, `⚠️ ${result.error}`);
        await bot.sendDocument(chatId, result.pdfPath, {
          caption: `📝 *Quote ${result.quoteId}*\n👤 ${result.customer}\n💰 ${formatAmount(result.total, companyProfiles[userId]?.currency || 'AED')}\n\n_Convert to invoice when the client approves._`,
          parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: [
            [{ text: '🔄 Convert to Invoice', callback_data: `quote_convert_${result.quoteId}` }],
            [{ text: '📝 My Quotes', callback_data: 'nav_quotes' }, { text: '🏠 Home', callback_data: 'nav_home' }]
          ]}
        });
        try { fs.unlinkSync(result.pdfPath); } catch (_) {}
      }
      else if (data.startsWith('quote_convert_')) {
        const quoteId = data.replace('quote_convert_', '');
        await send(chatId, '📄 _Converting quote to invoice..._');
        const result = await convertQuoteToInvoice(userId, quoteId);
        if (result.error) return send(chatId, `⚠️ ${result.error}`);
        await bot.sendDocument(chatId, result.pdfPath, {
          caption: `✅ *Converted!*\n📋 \`${result.invoiceId}\`\n👤 ${result.customer}\n💰 ${formatAmount(result.total, result.currency)}${result.paymentUrl ? `\n\n💳 ${result.paymentUrl}` : ''}`,
          parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: [[{ text: '✅ Mark as Paid', callback_data: `paid_${result.invoiceId}` }, { text: '🏠 Home', callback_data: 'nav_home' }]] }
        });
        try { fs.unlinkSync(result.pdfPath); } catch (_) {}
      }
      else if (data.startsWith('svc_del_'))  {
        const svcId = data.replace('svc_del_', '');
        deleteService(userId, svcId);
        showServices(chatId, userId);
      }
      else if (data === 'svc_add') {
        commandState[userId] = { type: 'svc_add' };
        send(chatId, `➕ *Add Service*\n\nType the service name and price:\n_"Website Design · 5000"_\n_"Monthly Retainer · 3000"_`);
      }
      else if (data.startsWith('client_del_')) {
        const name = decodeURIComponent(data.replace('client_del_', ''));
        deleteClient(userId, name);
        showClients(chatId, userId);
      }
      else if (data.startsWith('rec_toggle_')) {
        const recId = data.replace('rec_toggle_', '');
        const isActive = pauseRecurring(userId, recId);
        send(chatId, isActive ? '▶ Recurring invoice *resumed*.' : '⏸ Recurring invoice *paused*.', { parse_mode: 'Markdown' });
        showRecurring(chatId, userId);
      }
      else if (data.startsWith('rec_del_')) {
        const recId = data.replace('rec_del_', '');
        deleteRecurring(userId, recId);
        send(chatId, '❌ Recurring invoice deleted.');
        showRecurring(chatId, userId);
      }
      else if (data.startsWith('rec_freq_')) {
        const parts = data.replace('rec_freq_', '').split('_');
        const frequency = parts[0];
        const invoiceId = parts.slice(1).join('_');
        handleRecurringFrequency(chatId, userId, frequency, invoiceId);
      }
      else if (data.startsWith('recurring_setup_')) {
        handleRecurringSetup(chatId, userId, data.replace('recurring_setup_', ''));
      }
      else if (data.startsWith('vat_')) {
        const [, q, y] = data.split('_');
        handleVatReport(chatId, userId, q, y);
      }
      else if (data === 'credit_new') {
        commandState[userId] = { type: 'credit_invoice_id' };
        send(chatId, '🔴 *Issue Credit Note*\n\nEnter the invoice ID to credit:\n_e.g. INV-2026-0001_');
      }
      else if (data.startsWith('brand_color_')) {
        const colorKey = data.replace('brand_color_', '');
        const color = BRANDING_COLORS[colorKey];
        if (color) {
          saveBranding(userId, { accentColor: color.hex });
          send(chatId, `🎨 *Color updated to ${color.name}!*\n\nYour next invoice will use this accent color.`,
            { reply_markup: { inline_keyboard: [[{ text: '🎨 Branding', callback_data: 'cmd_branding' }]] }}
          );
        }
      }
      else if (data === 'brand_thankyou') {
        commandState[userId] = { type: 'brand_thankyou' };
        send(chatId, `💬 *Thank-You Message*\n\nType your thank-you message _(max 120 chars)_:\n_e.g. "Thank you for your business! We appreciate your trust." 🙏_`);
      }
      else if (data === 'brand_footer') {
        commandState[userId] = { type: 'brand_footer' };
        send(chatId, `📝 *Footer Note*\n\nType your footer note _(max 80 chars)_:\n_e.g. "Payment due within 30 days. Late fee 2%."_`);
      }
      else if (data === 'brand_reset') {
        resetBranding(userId);
        send(chatId, '🔄 Branding reset to default *(Indigo)*. Future invoices will use the standard InvoKash design.',
          { reply_markup: { inline_keyboard: [[{ text: '🎨 Branding', callback_data: 'cmd_branding' }]] }}
        );
      }
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
