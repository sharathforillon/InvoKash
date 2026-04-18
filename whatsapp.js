/**
 * InvoKash — WhatsApp Business Cloud API Handler (v2)
 * Handles incoming WhatsApp messages via Meta webhook
 * Supports: text, voice, interactive buttons/lists
 *
 * Setup: https://developers.facebook.com/docs/whatsapp/cloud-api/get-started
 */

require('dotenv').config();
const express = require('express');
const axios   = require('axios');
const fs      = require('fs');
const path    = require('path');

const {
  companyProfiles, invoiceHistory, onboardingState, commandState, pendingInvoices,
  CURRENCIES, PERIOD_NAMES, LOGO_DIR, RECEIPTS_DIR,
  checkRateLimit, sanitizeInput, formatAmount, getTaxConfig,
  filterInvoicesByPeriod, progressBar, asciiBar, calculateStats,
  classifyIntent, transcribeAudio, validateInvoiceData,
  processInvoiceText, confirmInvoice, markInvoicePaid,
  buildDownloadZip, saveData, generateCSV,
  getAgingReport, getRevenueGoal, setRevenueGoal,
  extractExpenseFromImage, extractExpenseFromPDF, logExpense, getExpenses, calculateProfitLoss,
} = require('./core');

const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET;

const WHATSAPP_TOKEN   = process.env.WHATSAPP_TOKEN;
const PHONE_ID         = process.env.WHATSAPP_PHONE_ID;
const VERIFY_TOKEN     = process.env.WHATSAPP_VERIFY_TOKEN || 'invokash_webhook_secret';
const PORT             = parseInt(process.env.PORT || '3000');
const GRAPH_URL        = 'https://graph.facebook.com/v19.0';

const app = express();
app.use(express.json());

// ─── WhatsApp State (keyed by phone number, prefix 'wa_') ────────────────────
// Note: uses same companyProfiles / invoiceHistory from core.js but with 'wa_' prefix
const waOnboarding = {};  // { 'wa_phone': { step, ... } }
const waCmd        = {};  // { 'wa_phone': { type } }

// ─── WhatsApp API Helpers ─────────────────────────────────────────────────────
async function waSend(to, text) {
  if (!WHATSAPP_TOKEN || !PHONE_ID) return;
  // WhatsApp has a 4096 char limit; split if needed
  const chunks = splitText(text, 4000);
  for (const chunk of chunks) {
    await axios.post(`${GRAPH_URL}/${PHONE_ID}/messages`, {
      messaging_product: 'whatsapp',
      to,
      type: 'text',
      text: { body: chunk, preview_url: false },
    }, {
      headers: { Authorization: `Bearer ${WHATSAPP_TOKEN}`, 'Content-Type': 'application/json' },
      timeout: 15000,
    }).catch(err => console.error('WA send error:', err.response?.data || err.message));
    if (chunks.length > 1) await new Promise(r => setTimeout(r, 200)); // rate limit
  }
}

// Send interactive buttons (max 3)
async function waSendButtons(to, bodyText, buttons, headerText = null) {
  if (!WHATSAPP_TOKEN || !PHONE_ID) return;
  const payload = {
    messaging_product: 'whatsapp',
    to,
    type: 'interactive',
    interactive: {
      type: 'button',
      body: { text: bodyText.slice(0, 1024) },
      action: {
        buttons: buttons.slice(0, 3).map(b => ({
          type: 'reply',
          reply: { id: b.id.slice(0, 256), title: b.title.slice(0, 20) },
        })),
      },
    },
  };
  if (headerText) payload.interactive.header = { type: 'text', text: headerText.slice(0, 60) };

  await axios.post(`${GRAPH_URL}/${PHONE_ID}/messages`, payload, {
    headers: { Authorization: `Bearer ${WHATSAPP_TOKEN}`, 'Content-Type': 'application/json' },
    timeout: 15000,
  }).catch(err => console.error('WA buttons error:', err.response?.data || err.message));
}

// Send list message (up to 10 items)
async function waSendList(to, bodyText, buttonText, sections) {
  if (!WHATSAPP_TOKEN || !PHONE_ID) return;
  await axios.post(`${GRAPH_URL}/${PHONE_ID}/messages`, {
    messaging_product: 'whatsapp',
    to,
    type: 'interactive',
    interactive: {
      type: 'list',
      body: { text: bodyText },
      action: { button: buttonText, sections },
    },
  }, {
    headers: { Authorization: `Bearer ${WHATSAPP_TOKEN}`, 'Content-Type': 'application/json' },
    timeout: 15000,
  }).catch(err => console.error('WA list error:', err.response?.data || err.message));
}

// Upload media to WhatsApp and send as document
async function waSendDocument(to, filePath, filename, caption = '') {
  if (!WHATSAPP_TOKEN || !PHONE_ID) return;
  try {
    const FormData = require('form-data');
    const form     = new FormData();
    form.append('file', fs.createReadStream(filePath), { filename, contentType: 'application/pdf' });
    form.append('type', 'application/pdf');
    form.append('messaging_product', 'whatsapp');

    const upload = await axios.post(`${GRAPH_URL}/${PHONE_ID}/media`, form, {
      headers: { Authorization: `Bearer ${WHATSAPP_TOKEN}`, ...form.getHeaders() },
      timeout: 60000,
    });

    const mediaId = upload.data.id;
    await axios.post(`${GRAPH_URL}/${PHONE_ID}/messages`, {
      messaging_product: 'whatsapp',
      to,
      type: 'document',
      document: { id: mediaId, filename, caption: caption.slice(0, 1024) },
    }, {
      headers: { Authorization: `Bearer ${WHATSAPP_TOKEN}`, 'Content-Type': 'application/json' },
      timeout: 30000,
    });
  } catch (err) {
    console.error('WA document send error:', err.response?.data || err.message);
    // Fallback: just send the URL if media upload fails
    await waSend(to, `📄 ${caption}\n\n_(Attach manually: file generated as ${filename})_`);
  }
}

// Download media from WhatsApp
async function waDownloadMedia(mediaId) {
  const infoRes = await axios.get(`${GRAPH_URL}/${mediaId}`, {
    headers: { Authorization: `Bearer ${WHATSAPP_TOKEN}` },
    timeout: 15000,
  });
  const mediaUrl = infoRes.data.url;
  const dlRes = await axios.get(mediaUrl, {
    headers: { Authorization: `Bearer ${WHATSAPP_TOKEN}` },
    responseType: 'arraybuffer',
    timeout: 60000,
  });
  return Buffer.from(dlRes.data);
}

// Split long text into chunks
function splitText(text, maxLen) {
  if (text.length <= maxLen) return [text];
  const chunks = [];
  let i = 0;
  while (i < text.length) {
    let end = Math.min(i + maxLen, text.length);
    // Try to break at newline
    if (end < text.length) {
      const nl = text.lastIndexOf('\n', end);
      if (nl > i) end = nl + 1;
    }
    chunks.push(text.slice(i, end));
    i = end;
  }
  return chunks;
}

// Strip Telegram-style Markdown (WhatsApp uses different formatting)
function stripMarkdown(text) {
  return text
    .replace(/\*([^*]+)\*/g, '*$1*')   // keep bold (WhatsApp supports *bold*)
    .replace(/_([^_]+)_/g, '_$1_')     // keep italic (WhatsApp supports _italic_)
    .replace(/`([^`]+)`/g, '$1')       // remove code ticks
    .replace(/━+/g, '─────────────')   // replace thick lines with thin
    .trim();
}

// ─── Webhook Verification (GET) ───────────────────────────────────────────────
app.get('/webhook', (req, res) => {
  const mode      = req.query['hub.mode'];
  const token     = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];
  if (mode === 'subscribe' && token === VERIFY_TOKEN) {
    console.log('✅ WhatsApp webhook verified');
    return res.status(200).send(challenge);
  }
  res.sendStatus(403);
});

// ─── Incoming Messages (POST) ─────────────────────────────────────────────────
app.post('/webhook', async (req, res) => {
  res.sendStatus(200); // respond immediately to Meta

  try {
    const body = req.body;
    if (body.object !== 'whatsapp_business_account') return;

    const entry   = body.entry?.[0];
    const changes = entry?.changes?.[0];
    const value   = changes?.value;
    if (!value?.messages?.length) return; // ignore status updates

    const msg  = value.messages[0];
    const from = msg.from; // phone number (userId = 'wa_' + from)
    const userId = `wa_${from}`;

    if (!checkRateLimit(userId)) {
      return waSend(from, '⏱ Too many messages — please wait a moment and try again.');
    }

    // ── Interactive reply (button or list selection) ──────────────────────────
    if (msg.type === 'interactive') {
      const replyId = msg.interactive?.button_reply?.id || msg.interactive?.list_reply?.id;
      if (replyId) await handleWaCallback(from, userId, replyId);
      return;
    }

    // ── Audio / Voice ─────────────────────────────────────────────────────────
    if (msg.type === 'audio') {
      await handleWaVoice(from, userId, msg.audio);
      return;
    }

    // ── Image (for logo upload during onboarding) ─────────────────────────────
    if (msg.type === 'image' && waOnboarding[userId]?.step === 'logo') {
      await handleWaLogoUpload(from, userId, msg.image);
      return;
    }

    // ── Image (receipt scanning for expense logging) ─────────────────────────
    if (msg.type === 'image' && companyProfiles[userId]) {
      const caption = msg.image?.caption || '';
      await handleWaReceiptImage(from, userId, msg.image, caption);
      return;
    }

    // ── Document (PDF/image receipt scanning) ────────────────────────────────
    if (msg.type === 'document' && companyProfiles[userId]) {
      const caption = msg.document?.caption || '';
      await handleWaReceiptDocument(from, userId, msg.document, caption);
      return;
    }

    // ── Text ──────────────────────────────────────────────────────────────────
    const text = msg.text?.body?.trim() || '';
    if (!text) return;

    // Not set up yet
    if (!companyProfiles[userId] && !waOnboarding[userId]) {
      const name = value.contacts?.[0]?.profile?.name || 'there';
      await waShowLanding(from, name);
      return;
    }

    // Active onboarding
    if (waOnboarding[userId]) {
      await waHandleOnboarding(from, userId, text);
      return;
    }

    // Command state
    if (waCmd[userId]) {
      await waHandleCommandState(from, userId, text);
      return;
    }

    // Free text
    await waHandleText(from, userId, text, value.contacts?.[0]?.profile?.name || 'there');

  } catch (err) {
    console.error('WA webhook error:', err.message);
  }
});

// ─── Health Check ─────────────────────────────────────────────────────────────
app.get('/health', (_, res) => res.json({ status: 'ok', service: 'InvoKash WhatsApp', ts: new Date().toISOString() }));

// ─── Landing / Welcome ────────────────────────────────────────────────────────
async function waShowLanding(to, firstName) {
  await waSend(to,
    `👋 Welcome to *InvoKash*, ${firstName}!\n\n` +
    `🤖 Your AI invoice assistant — create professional invoices in seconds by voice or text.\n\n` +
    `✅ Voice & text invoice creation\n` +
    `✅ Multi-currency support\n` +
    `✅ VAT & GST compliant PDFs\n` +
    `✅ Instant payment links\n\n` +
    `Reply *SETUP* to set up your account (takes ~2 minutes), or *HELP* to learn more.`
  );
}

async function waShowWelcome(to, userId) {
  const profile = companyProfiles[userId];
  if (!profile) return waShowLanding(to, 'there');

  const history    = invoiceHistory[userId] || [];
  const thisMonth  = filterInvoicesByPeriod(history, 'this_month');
  const monthStats = calculateStats(thisMonth, profile.currency);

  const curr = CURRENCIES[profile.currency] || {};
  let msg = `🏠 *${profile.company_name}*\n${curr.flag || ''} ${profile.currency} · ${profile.company_address || ''}\n\n`;

  if (history.length > 0) {
    msg += `📊 *This Month:* ${monthStats.count} invoices · ${formatAmount(monthStats.total, profile.currency)}\n`;
    if (monthStats.unpaid > 0) msg += `⏳ Unpaid: ${formatAmount(monthStats.unpaid, profile.currency)}\n`;
    msg += '\n';
  }

  msg += `💬 *Create an Invoice*\nJust type or send a voice message:\n"Website design for Acme Corp for 3500"\n\n`;
  msg += `📋 *Commands*\nSTATS · INVOICES · CUSTOMERS · DOWNLOAD · PROFILE · HELP`;

  await waSend(to, msg);
}

// ─── Onboarding ───────────────────────────────────────────────────────────────
const WA_ONBOARD_TOTAL = 10;

function waStartOnboarding(to, userId, firstName = 'there') {
  const isUpdate = !!companyProfiles[userId];
  waOnboarding[userId] = { step: 'disclaimer' };

  waSend(to,
    (isUpdate ? `⚙️ *Update Your Profile*\n\n` : `🎉 Let\'s set up your account, ${firstName}!\n\n`) +
    `⚠️ *Disclaimer*\n\n` +
    `InvoKash generates invoices for *record-keeping purposes only*. Not legally certified tax documents.\n\n` +
    `By proceeding you confirm:\n` +
    `• You handle tax compliance in your jurisdiction\n` +
    `• Your data is stored securely and not shared\n` +
    `• You can delete data anytime by typing DELETEDATA\n\n` +
    `Reply *AGREE* to continue or *CANCEL* to stop.`
  );
}

async function waHandleLogoUpload(to, userId, image) {
  try {
    const buffer   = await waDownloadMedia(image.id);
    const logoPath = path.join(LOGO_DIR, `logo_${userId}.jpg`);
    fs.writeFileSync(logoPath, buffer);
    companyProfiles[userId].logo_path = logoPath;
    saveData();
  } catch (err) {
    console.error('WA logo error:', err.message);
    companyProfiles[userId].logo_path = null;
  }
  delete waOnboarding[userId];
  await waSendSetupComplete(to, userId);
}

async function waHandleOnboarding(to, userId, text) {
  const state = waOnboarding[userId];
  if (!state) return;

  if (!companyProfiles[userId]) companyProfiles[userId] = {};
  const input = text.toLowerCase().trim();
  const p     = companyProfiles[userId];

  if (input === 'cancel') {
    delete waOnboarding[userId];
    return waSend(to, '❌ Setup cancelled. Type SETUP to restart any time.');
  }

  switch (state.step) {
    case 'disclaimer':
      if (input !== 'agree') return waSend(to, 'Please reply *AGREE* to continue or *CANCEL* to stop.');
      state.step = 'company_name';
      waSend(to, `${progressBar(1, WA_ONBOARD_TOTAL)}\n\n🏢 *Step 1 — Company Name*\n\nWhat is your business name?`);
      break;

    case 'company_name':
      if (!text.trim()) return waSend(to, '⚠️ Please enter a valid company name.');
      p.company_name = sanitizeInput(text);
      state.step = 'company_address';
      waSend(to, `${progressBar(2, WA_ONBOARD_TOTAL)}\n\n📍 *Step 2 — Business Address*\n\nEnter your full address, or type SKIP:`);
      break;

    case 'company_address':
      p.company_address = input === 'skip' ? '' : sanitizeInput(text);
      state.step = 'trn';
      waSend(to, `${progressBar(3, WA_ONBOARD_TOTAL)}\n\n🔐 *Step 3 — Tax Registration Number*\n\nEnter your TRN/VAT/GST number, or type SKIP:`);
      break;

    case 'trn':
      p.trn = input === 'skip' ? '' : sanitizeInput(text);
      state.step = 'currency';
      waSend(to,
        `${progressBar(4, WA_ONBOARD_TOTAL)}\n\n💰 *Step 4 — Currency*\n\nReply with your currency code:\n\n` +
        `🇦🇪 AED  🇺🇸 USD  🇪🇺 EUR  🇬🇧 GBP\n` +
        `🇮🇳 INR  🇸🇦 SAR  🇴🇲 OMR  🇶🇦 QAR\n` +
        `🇰🇼 KWD  🇧🇭 BHD  🇸🇬 SGD  🇨🇦 CAD\n` +
        `🇦🇺 AUD  🇪🇬 EGP`
      );
      break;

    case 'currency': {
      const curr = text.toUpperCase().trim();
      if (!CURRENCIES[curr]) return waSend(to, '⚠️ Invalid currency. Please reply with a 3-letter code like AED, USD, EUR, INR.');
      p.currency = curr;
      state.step = 'bank_name';
      waSend(to, `${progressBar(5, WA_ONBOARD_TOTAL)}\n\n🏦 *Step 5 — Bank Name*\n\nEnter your bank name (e.g. Emirates NBD, HDFC):`);
      break;
    }

    case 'bank_name':
      p.bank_name = sanitizeInput(text);
      state.step = 'iban';
      waSend(to, `${progressBar(6, WA_ONBOARD_TOTAL)}\n\n🔑 *Step 6 — ${p.currency === 'INR' ? 'Account & IFSC' : 'IBAN'}*\n\nEnter your ${p.currency === 'INR' ? 'account number and IFSC code' : 'IBAN'}:`);
      break;

    case 'iban':
      p.iban = sanitizeInput(text);
      state.step = 'account_name';
      waSend(to, `${progressBar(7, WA_ONBOARD_TOTAL)}\n\n👤 *Step 7 — Account Holder Name*\n\nName as it appears on the bank account:`);
      break;

    case 'account_name':
      p.account_name = sanitizeInput(text);
      state.step = 'tax_enabled';
      const taxType = CURRENCIES[p.currency]?.tax || 'VAT';
      await waSendButtons(to,
        `${progressBar(8, WA_ONBOARD_TOTAL)}\n\n📊 *Step 8 — Tax Settings*\n\nDo you charge ${taxType} on your invoices?`,
        [
          { id: 'tax_yes', title: `Yes, charge ${taxType}` },
          { id: 'tax_no',  title: 'No tax' },
        ]
      );
      break;

    case 'tax_enabled': {
      const taxField = ['INR','SGD','AUD'].includes(p.currency) ? 'gst' : 'vat';
      if (input === 'yes' || input === 'tax_yes') {
        p[`${taxField}_enabled`] = true;
        if (taxField === 'gst') { p.vat_enabled = false; p.vat_rate = 0; }
        else                    { p.gst_enabled = false; p.gst_rate = 0; }
        state.step = 'tax_rate';
        waSend(to, `${progressBar(9, WA_ONBOARD_TOTAL)}\n\n📈 *Step 9 — ${taxField.toUpperCase()} Rate*\n\nEnter the percentage (e.g. 5 for 5%):`);
      } else if (input === 'no' || input === 'tax_no') {
        p.vat_enabled = false; p.vat_rate = 0;
        p.gst_enabled = false; p.gst_rate = 0;
        state.step = 'logo';
        waSend(to,
          `${progressBar(10, WA_ONBOARD_TOTAL)}\n\n🖼 *Step 10 — Company Logo (Optional)*\n\n` +
          `Send your logo image (JPG/PNG), or reply SKIP:`
        );
      } else {
        waSendButtons(to, 'Please select an option:',
          [{ id: 'tax_yes', title: 'Yes, charge tax' }, { id: 'tax_no', title: 'No tax' }]);
      }
      break;
    }

    case 'tax_rate': {
      const rate = parseFloat(text);
      if (isNaN(rate) || rate < 0 || rate > 100) return waSend(to, '⚠️ Please enter a number between 0 and 100 (e.g., 5).');
      const taxField = ['INR','SGD','AUD'].includes(p.currency) ? 'gst' : 'vat';
      p[`${taxField}_rate`] = rate;
      state.step = 'logo';
      waSend(to,
        `${progressBar(10, WA_ONBOARD_TOTAL)}\n\n🖼 *Step 10 — Company Logo (Optional)*\n\n` +
        `Send your logo image, or reply SKIP:`
      );
      break;
    }

    case 'logo':
      if (input === 'skip') {
        p.logo_path = null;
        delete waOnboarding[userId];
        saveData();
        await waSendSetupComplete(to, userId);
      }
      break;
  }
}

async function waSendSetupComplete(to, userId) {
  const p    = companyProfiles[userId];
  const tc   = getTaxConfig(p);
  const curr = CURRENCIES[p.currency] || {};
  await waSend(to,
    `🎉 *Setup Complete!*\n\n` +
    `*${p.company_name}*\n` +
    `📍 ${p.company_address || 'No address'}\n` +
    `${curr.flag || ''} ${p.currency} · ${curr.name || ''}\n` +
    `📊 ${tc.type}: ${tc.enabled ? `${tc.rate}%` : 'Not charged'}\n` +
    `🏦 ${p.bank_name || 'No bank'}\n\n` +
    `You\'re all set! Just type or send a voice note to create an invoice.\n\n` +
    `_Example: "Web design for Acme Corp for 3000"_`
  );
}

// ─── Interactive Callback Handler ─────────────────────────────────────────────
async function handleWaCallback(to, userId, id) {
  // Onboarding buttons
  if (id === 'tax_yes' || id === 'tax_no') {
    await waHandleOnboarding(to, userId, id);
    return;
  }

  // Stats period
  if (id.startsWith('stats_')) {
    delete waCmd[userId];
    await waShowStats(to, userId, id.replace('stats_', ''));
    return;
  }

  // Download period
  if (id.startsWith('dl_')) {
    delete waCmd[userId];
    await waDownloadInvoices(to, userId, id.replace('dl_', ''));
    return;
  }

  // Invoice confirmation
  if (id === 'confirm_invoice') {
    await waHandleConfirmInvoice(to, userId);
    return;
  }
  if (id === 'retry_invoice') {
    delete pendingInvoices[userId];
    waSend(to, '🔄 Let\'s try again. Describe your invoice:\n_"Plumbing for Ahmed at Marina for 500"_');
    return;
  }

  // Receipt confirm/cancel
  if (id === 'wa_rcpt_confirm') {
    const state = waCmd[userId];
    if (state?.type === 'receipt_confirm' && state.expenseData) {
      const expense  = logExpense(userId, state.expenseData);
      delete waCmd[userId];
      const currency = companyProfiles[userId]?.currency || 'AED';
      const merchantNote = expense.merchant ? `  · 🏪 ${expense.merchant}` : '';
      const commentNote  = expense.comment  ? `\n💬 _${expense.comment}_`  : '';
      waSend(to,
        `✅ *Expense Logged*\n─────────────────────\n\n` +
        `📝 ${expense.description}${merchantNote}\n` +
        `🏷 ${expense.category}  · 📅 ${expense.date}\n` +
        `💰 *${formatAmount(expense.amount, currency)}*${commentNote}\n` +
        `📸 Receipt saved`
      );
    }
    return;
  }
  if (id === 'wa_rcpt_cancel') {
    const state = waCmd[userId];
    if (state?.type === 'receipt_confirm' && state.expenseData?.receipt_path) {
      try { fs.unlinkSync(state.expenseData.receipt_path); } catch (_) {}
    }
    delete waCmd[userId];
    waSend(to, '❌ Receipt discarded.');
    return;
  }

  // Mark as paid
  if (id.startsWith('paid_')) {
    const invoiceId = id.replace('paid_', '');
    const result    = markInvoicePaid(userId, invoiceId);
    waSend(to, result ? `✅ *${invoiceId}* marked as paid! Great work! 💪` : `⚠️ Invoice ${invoiceId} not found.`);
    return;
  }

  // Navigation
  if (id === 'nav_home')      await waShowWelcome(to, userId);
  if (id === 'nav_stats')     await waShowPeriodSelector(to, userId, 'stats');
  if (id === 'nav_download')  await waDownloadInvoices(to, userId, 'this_month');
  if (id === 'nav_invoices')  await waShowInvoices(to, userId);
  if (id === 'nav_customers') await waShowCustomers(to, userId);
  if (id === 'nav_profile')   await waShowProfile(to, userId);
}

// ─── Text Router ──────────────────────────────────────────────────────────────
async function waHandleText(to, userId, text, firstName) {
  const upper = text.trim().toUpperCase();
  const lower = text.toLowerCase();

  // Keyword commands
  if (upper === 'SETUP' || upper === 'START')    return waStartOnboarding(to, userId, firstName);
  if (upper === 'HELP')                          return waShowHelp(to);
  if (upper === 'HOME')                          return waShowWelcome(to, userId);
  if (upper === 'PROFILE')                       return waShowProfile(to, userId);
  if (upper === 'INVOICES')                      return waShowInvoices(to, userId);
  if (upper === 'CUSTOMERS')                     return waShowCustomers(to, userId);
  if (upper === 'STATS')                         return waShowPeriodSelector(to, userId, 'stats');
  if (upper === 'DOWNLOAD')                      return waShowPeriodSelector(to, userId, 'download');
  if (upper === 'DELETEDATA')                    return waConfirmDeleteData(to, userId);
  if (upper === 'AGING')                         return waShowAging(to, userId);
  if (upper === 'GOAL')                          return waShowGoal(to, userId);
  if (upper === 'EXPENSES')                      return waShowExpenses(to, userId);
  if (upper === 'PROFIT' || upper === 'PNL')     return waShowProfit(to, userId);

  // Natural language period commands
  if (/\b(stat|revenue|earn)/i.test(lower)) {
    const period = /this month/i.test(lower) ? 'this_month' : /last month/i.test(lower) ? 'last_month' : null;
    if (period) return waShowStats(to, userId, period);
    return waShowPeriodSelector(to, userId, 'stats');
  }
  if (/\bdownload\b/i.test(lower)) {
    const period = /this month/i.test(lower) ? 'this_month' : /last month/i.test(lower) ? 'last_month' : null;
    if (period) return waDownloadInvoices(to, userId, period);
    return waShowPeriodSelector(to, userId, 'download');
  }

  // AI intent classification + invoice handling
  const intent = await classifyIntent(text);
  if (intent === 'invoice') {
    await waHandleInvoiceRequest(to, userId, sanitizeInput(text));
  } else if (intent === 'greeting' || intent === 'help') {
    waShowWelcome(to, userId);
  } else if (intent === 'stats') {
    waShowPeriodSelector(to, userId, 'stats');
  } else if (intent === 'download') {
    waShowPeriodSelector(to, userId, 'download');
  } else {
    waSend(to,
      `❓ I didn't understand that.\n\n` +
      `To create an invoice, try:\n"Web design for Acme Corp for 3000"\n\n` +
      `Or type *HELP* to see all commands.`
    );
  }
}

async function waHandleCommandState(to, userId, text) {
  const state  = waCmd[userId];
  if (!state) return;
  const lower  = text.toLowerCase();
  const period = /this month/i.test(lower) ? 'this_month'
               : /last month/i.test(lower) ? 'last_month'
               : /quarter/i.test(lower)    ? 'this_quarter'
               : /this year/i.test(lower)  ? 'this_year'
               : /\ball\b/i.test(lower)    ? 'all' : null;
  if (period) {
    delete waCmd[userId];
    if (state.type === 'stats') await waShowStats(to, userId, period);
    else await waDownloadInvoices(to, userId, period);
  } else {
    waSend(to, 'Reply with a period: *this month*, *last month*, *this quarter*, *this year*, or *all*');
  }
}

// ─── Voice Messages ───────────────────────────────────────────────────────────
async function handleWaVoice(to, userId, audio) {
  try {
    await waSend(to, '🎤 Transcribing your voice note...');
    const buffer    = await waDownloadMedia(audio.id);
    const voicePath = `/tmp/voice/wa_${userId.replace(/\W/g,'_')}_${Date.now()}.ogg`;
    fs.writeFileSync(voicePath, buffer);

    let transcribed;
    try {
      transcribed = await transcribeAudio(voicePath);
    } finally {
      try { fs.unlinkSync(voicePath); } catch (_) {}
    }

    transcribed = sanitizeInput(transcribed);
    await waSend(to, `🎤 *Heard:* "${transcribed}"\n\n⚡ Processing...`);
    await waHandleText(to, userId, transcribed, 'there');
  } catch (err) {
    console.error('WA voice error:', err.message);
    waSend(to, '⚠️ Could not process voice message. Please type your invoice details.');
  }
}

// ─── Receipt Image Handler (WhatsApp) ─────────────────────────────────────────
async function handleWaReceiptImage(to, userId, image, caption) {
  await waSend(to, '📸 Scanning receipt...');
  let receiptPath = null;
  try {
    const buffer = await waDownloadMedia(image.id);
    const receiptFilename = `receipt_${userId}_${Date.now()}.jpg`;
    receiptPath = path.join(RECEIPTS_DIR, receiptFilename);
    fs.writeFileSync(receiptPath, buffer);

    const data     = await extractExpenseFromImage(buffer);
    const currency = companyProfiles[userId].currency;

    if (!data.amount || parseFloat(data.amount) <= 0) {
      try { fs.unlinkSync(receiptPath); } catch (_) {}
      return waSend(to,
        '⚠️ Couldn\'t read a total amount from this receipt.\n\n' +
        'Try typing the expense instead:\n"Spent 500 on petrol"'
      );
    }

    const expenseData = { ...data, receipt_path: receiptPath };
    if (caption) expenseData.comment = caption;
    waCmd[userId] = { type: 'receipt_confirm', expenseData };

    const merchantLine = data.merchant ? `🏪 *${data.merchant}*\n` : '';
    const dateLine     = data.date     ? `📅 ${data.date}\n`       : '';
    const commentLine  = caption       ? `💬 _${caption}_\n`       : '';
    const msg =
      `📸 *Receipt Scanned*\n─────────────────────\n\n` +
      `${merchantLine}` +
      `📝 ${data.description}\n` +
      `🏷 Category: *${data.category}*\n` +
      `💰 *${formatAmount(data.amount, currency)}*\n` +
      `${dateLine}` +
      `${commentLine}\n` +
      `_Image saved for tax records._`;

    await waSendButtons(to, msg, [
      { id: 'wa_rcpt_confirm', title: '✅ Log Expense' },
      { id: 'wa_rcpt_cancel',  title: '❌ Discard' },
    ]);
  } catch (err) {
    console.error('WA receipt scan error:', err.message);
    if (receiptPath) { try { fs.unlinkSync(receiptPath); } catch (_) {} }
    waSend(to,
      '⚠️ Couldn\'t read the receipt. Try typing the expense instead:\n"Spent 300 on coffee"'
    );
  }
}

// ─── Receipt Document Handler (WhatsApp) ──────────────────────────────────────
async function handleWaReceiptDocument(to, userId, doc, caption) {
  const mime    = doc.mime_type || '';
  const isPDF   = mime === 'application/pdf';
  const isImage = mime.startsWith('image/');

  if (!isPDF && !isImage) {
    return waSend(to,
      '⚠️ I can scan *PDF documents* and *images* (JPEG, PNG).\n\n' +
      'Send a receipt, flight ticket, or invoice to auto-log it as an expense.'
    );
  }

  await waSend(to, isPDF ? '📄 Reading document...' : '📸 Scanning image...');
  let receiptPath = null;
  try {
    const buffer = await waDownloadMedia(doc.id);
    const extMap = { 'application/pdf': '.pdf', 'image/jpeg': '.jpg', 'image/png': '.png', 'image/webp': '.webp' };
    const ext    = extMap[mime] || (isPDF ? '.pdf' : '.jpg');
    const fname  = `receipt_${userId}_${Date.now()}${ext}`;
    receiptPath  = path.join(RECEIPTS_DIR, fname);
    fs.writeFileSync(receiptPath, buffer);

    const data     = isPDF ? await extractExpenseFromPDF(buffer) : await extractExpenseFromImage(buffer, mime);
    const currency = companyProfiles[userId].currency;

    if (!data.amount || parseFloat(data.amount) <= 0) {
      try { fs.unlinkSync(receiptPath); } catch (_) {}
      return waSend(to,
        '⚠️ Couldn\'t find a total amount in this document.\n\n' +
        'Try typing the expense instead:\n"Flight to Dubai 850"'
      );
    }

    const expenseData = { ...data, receipt_path: receiptPath };
    if (caption) expenseData.comment = caption;
    waCmd[userId] = { type: 'receipt_confirm', expenseData };

    const typeLabel    = isPDF ? '📄 *Document Scanned*' : '📸 *Image Scanned*';
    const merchantLine = data.merchant ? `🏪 *${data.merchant}*\n` : '';
    const dateLine     = data.date     ? `📅 ${data.date}\n`       : '';
    const commentLine  = caption       ? `💬 _${caption}_\n`       : '';
    const msg =
      `${typeLabel}\n─────────────────────\n\n` +
      `${merchantLine}` +
      `📝 ${data.description}\n` +
      `🏷 Category: *${data.category}*\n` +
      `💰 *${formatAmount(data.amount, currency)}*\n` +
      `${dateLine}` +
      `${commentLine}\n` +
      `_File saved for tax records._`;

    await waSendButtons(to, msg, [
      { id: 'wa_rcpt_confirm', title: '✅ Log Expense' },
      { id: 'wa_rcpt_cancel',  title: '❌ Discard' },
    ]);
  } catch (err) {
    console.error('WA document scan error:', err.message);
    if (receiptPath) { try { fs.unlinkSync(receiptPath); } catch (_) {} }
    waSend(to,
      '⚠️ Couldn\'t read this document. Try typing the expense instead:\n"Flight to London 1200"'
    );
  }
}

// ─── Invoice Flow ─────────────────────────────────────────────────────────────
async function waHandleInvoiceRequest(to, userId, text) {
  if (!companyProfiles[userId]) return waSend(to, '⚠️ Please type *SETUP* to set up your profile first.');

  await waSend(to, '⚡ Reading invoice details...');

  const result = await processInvoiceText(userId, text);

  if (result.error === 'no_profile') return waSend(to, '⚠️ Type *SETUP* to create your profile first.');
  if (result.error === 'parse_failed') {
    return waSend(to, '⚠️ Couldn\'t parse invoice details. Try:\n"[Service] for [Customer] for [Amount]"\n\nExample: "Web design for John Smith for 2000"');
  }
  if (result.error === 'validation') {
    return waSend(to, `⚠️ Missing information:\n${result.errors.map(e => `• ${e}`).join('\n')}\n\nExample: "Consulting for Ahmed Al-Rashidi for 1500"`);
  }

  const { pending } = result;
  const { data, profile, subtotal, tax, total, tc } = pending;

  let preview =
    `📋 *Invoice Preview*\n─────────────────────\n\n` +
    `🏢 From: ${profile.company_name}\n` +
    `👤 Bill To: ${data.customer_name}\n`;
  if (data.address && data.address !== 'null') preview += `📍 ${data.address}\n`;
  preview += `\nServices:\n`;
  data.line_items.forEach(item => {
    preview += `• ${item.description}: ${formatAmount(item.amount, profile.currency)}\n`;
  });
  preview += `\n─────────────────────\n`;
  if (tc.enabled && tax > 0) preview += `Subtotal: ${formatAmount(subtotal, profile.currency)}\n${tc.type} (${tc.rate}%): ${formatAmount(tax, profile.currency)}\n`;
  preview += `💰 *Total: ${formatAmount(total, profile.currency)}*`;

  await waSendButtons(to, preview,
    [
      { id: 'confirm_invoice', title: '✅ Generate Invoice' },
      { id: 'retry_invoice',   title: '🔄 Try Again' },
    ]
  );
}

async function waHandleConfirmInvoice(to, userId) {
  if (!pendingInvoices[userId]) return waSend(to, '⚠️ No pending invoice. Please describe your invoice again.');

  await waSend(to, '📄 Generating your PDF invoice...');

  try {
    const result = await confirmInvoice(userId);
    if (result.error) return waSend(to, '⚠️ Error generating invoice. Please try again.');

    let caption =
      `📄 ${result.invoiceId}\n` +
      `👤 ${result.customer}\n` +
      `💰 ${formatAmount(result.total, result.currency)}`;
    if (result.paymentUrl) caption += `\n\n💳 Pay Online:\n${result.paymentUrl}`;

    await waSendDocument(to, result.pdfPath, `${result.invoiceId}.pdf`, caption);
    try { fs.unlinkSync(result.pdfPath); } catch (_) {}

    // Follow-up buttons
    await waSendButtons(to, `Invoice sent! 🎉`,
      [{ id: `paid_${result.invoiceId}`, title: '✅ Mark as Paid' }]
    );

  } catch (err) {
    console.error('WA invoice confirm error:', err.message);
    waSend(to, '⚠️ Error generating invoice. Please try again.');
  }
}

// ─── View Screens ─────────────────────────────────────────────────────────────
function waShowHelp(to) {
  waSend(to,
    `📖 *InvoKash Help*\n\n` +
    `*Creating Invoices*\nType or send a voice note:\n` +
    `"Plumbing for Ahmed at Marina for 500"\n` +
    `"Web design for Acme Corp for 3000"\n\n` +
    `*Commands* (reply with keyword)\n` +
    `HOME — Home screen\n` +
    `SETUP — Set up or update profile\n` +
    `PROFILE — View your profile\n` +
    `INVOICES — Recent invoices\n` +
    `CUSTOMERS — Customer list\n` +
    `STATS — Revenue statistics\n` +
    `DOWNLOAD — Get invoices ZIP\n` +
    `AGING — Overdue invoice report\n` +
    `GOAL — Monthly revenue goal\n` +
    `EXPENSES — Track expenses\n` +
    `PROFIT — P&L report\n` +
    `DELETEDATA — Delete all data\n` +
    `HELP — This message\n\n` +
    `*Currencies:* AED USD EUR GBP INR SAR OMR KWD BHD QAR EGP SGD CAD AUD\n\n` +
    `💬 Support: @${process.env.SUPPORT_USERNAME || 'InvoKashSupport'}`
  );
}

async function waShowProfile(to, userId) {
  const p = companyProfiles[userId];
  if (!p) return waSend(to, '👤 No profile found. Type *SETUP* to create one.');

  const curr = CURRENCIES[p.currency] || {};
  const tc   = getTaxConfig(p);
  const invs = invoiceHistory[userId] || [];
  const stats = calculateStats(invs, p.currency);

  waSend(to,
    `👤 *Business Profile*\n─────────────────────\n\n` +
    `🏢 *${p.company_name}*\n` +
    `📍 ${p.company_address || 'Not set'}\n` +
    `🔐 TRN: ${p.trn || 'Not set'}\n\n` +
    `${curr.flag || ''} ${p.currency} — ${curr.name || ''}\n` +
    `📊 ${tc.type}: ${tc.enabled ? `${tc.rate}%` : 'Not charged'}\n\n` +
    `🏦 ${p.bank_name || 'No bank'}\n` +
    `🔑 ${p.iban || 'No IBAN'}\n` +
    `👤 ${p.account_name || 'No name'}\n\n` +
    `📈 *Lifetime:* ${stats.count} invoices · ${formatAmount(stats.total, p.currency)}\n` +
    `✅ Paid: ${formatAmount(stats.paid, p.currency)} · ⏳ Unpaid: ${formatAmount(stats.unpaid, p.currency)}\n\n` +
    `Type *SETUP* to update your profile.`
  );
}

async function waShowInvoices(to, userId) {
  const invs = invoiceHistory[userId] || [];
  if (invs.length === 0) return waSend(to, '📋 No invoices yet.\n\nCreate your first by typing:\n"Consulting for John Smith for 1500"');

  const currency = companyProfiles[userId]?.currency || 'AED';
  const recent   = invs.slice(-8).reverse();

  let msg = `📋 *Invoices* (${invs.length} total)\n─────────────────────\n\n`;
  recent.forEach((inv, i) => {
    const customer = inv.customer_name?.trim() || 'Unknown';
    const amount   = formatAmount(parseFloat(inv.total) || 0, inv.currency || currency);
    const status   = inv.status === 'paid' ? '✅' : '⏳';
    msg += `${status} ${inv.invoice_id}\n${customer} · ${amount} · ${inv.date}\n\n`;
  });

  // Show mark-paid options for unpaid
  const unpaid = recent.filter(i => i.status !== 'paid').slice(0, 2);
  if (unpaid.length > 0) {
    msg += `\nReply with invoice ID to mark as paid, e.g: *PAID ${unpaid[0].invoice_id}*`;
  }

  await waSend(to, msg);
}

async function waShowCustomers(to, userId) {
  const invs = invoiceHistory[userId] || [];
  if (invs.length === 0) return waSend(to, '👥 No customers yet. Create an invoice to add your first client!');

  const customers = {};
  invs.forEach(inv => {
    const name = inv.customer_name?.trim();
    if (!name) return;
    if (!customers[name]) customers[name] = { count: 0, total: 0, currency: inv.currency, last: inv.date };
    customers[name].count++;
    customers[name].total += parseFloat(inv.total) || 0;
    customers[name].last  = inv.date;
  });

  const sorted   = Object.entries(customers).sort((a, b) => b[1].total - a[1].total);
  const maxRevenue = sorted[0]?.[1].total || 1;

  let msg = `👥 *Customers* (${sorted.length} total)\n─────────────────────\n\n`;
  sorted.slice(0, 10).forEach(([name, d], i) => {
    const bar = asciiBar(d.total, maxRevenue, 8);
    msg += `${i + 1}. *${name}*\n${bar} ${formatAmount(d.total, d.currency)}\n${d.count} invoices · Last: ${d.last}\n\n`;
  });

  await waSend(to, msg);
}

async function waShowPeriodSelector(to, userId, type) {
  waCmd[userId] = { type };
  const icon  = type === 'stats' ? '📊' : '📥';
  const title = type === 'stats' ? 'Statistics' : 'Download Invoices';

  await waSendList(to,
    `${icon} *${title}*\n\nSelect a time period:`,
    'Select Period',
    [{
      title: 'Time Periods',
      rows: [
        { id: `${type === 'stats' ? 'stats' : 'dl'}_this_month`,   title: '📅 This Month'   },
        { id: `${type === 'stats' ? 'stats' : 'dl'}_last_month`,   title: '📅 Last Month'   },
        { id: `${type === 'stats' ? 'stats' : 'dl'}_this_quarter`, title: '📅 This Quarter' },
        { id: `${type === 'stats' ? 'stats' : 'dl'}_this_year`,    title: '📅 This Year'    },
        { id: `${type === 'stats' ? 'stats' : 'dl'}_all`,          title: '📅 All Time'     },
      ]
    }]
  );
}

async function waShowStats(to, userId, period) {
  const invs = invoiceHistory[userId] || [];
  if (invs.length === 0) return waSend(to, '📊 No invoices yet. Create your first to start tracking!');

  const filtered = filterInvoicesByPeriod(invs, period);
  if (filtered.length === 0) return waSend(to, `📊 No invoices for ${PERIOD_NAMES[period] || period}.`);

  const currency = companyProfiles[userId]?.currency || 'AED';
  const stats    = calculateStats(filtered, currency);

  let msg =
    `📊 *Statistics — ${PERIOD_NAMES[period] || period}*\n─────────────────────\n\n` +
    `📄 ${stats.count} invoices\n` +
    `💰 Revenue: ${formatAmount(stats.total, currency)}\n` +
    `📋 Subtotal: ${formatAmount(stats.subtotal, currency)}\n`;
  if (stats.taxTotal > 0) msg += `🏛 Tax: ${formatAmount(stats.taxTotal, currency)}\n`;
  msg += `📈 Average: ${formatAmount(stats.avg, currency)}\n`;
  msg += `✅ Paid: ${formatAmount(stats.paid, currency)}\n`;
  msg += `⏳ Unpaid: ${formatAmount(stats.unpaid, currency)}\n`;

  if (stats.topCustomers.length > 0) {
    const maxC   = stats.topCustomers[0][1];
    const medals = ['🥇', '🥈', '🥉', '4.', '5.'];
    msg += `\n🏆 *Top Customers*\n`;
    stats.topCustomers.forEach(([name, amt], i) => {
      const bar = asciiBar(amt, maxC, 8);
      msg += `${medals[i]} ${name}\n${bar} ${formatAmount(amt, currency)}\n`;
    });
  }

  await waSend(to, msg);
  await waSendButtons(to, 'Options:',
    [{ id: `dl_${period}`, title: '📥 Download ZIP' }]
  );
}

async function waDownloadInvoices(to, userId, period) {
  const invs = invoiceHistory[userId] || [];
  if (filterInvoicesByPeriod(invs, period).length === 0) {
    return waSend(to, `📥 No invoices for ${PERIOD_NAMES[period] || period}.`);
  }

  await waSend(to, '⏳ Preparing your download...');
  try {
    const result = await buildDownloadZip(userId, period);
    if (!result) return waSend(to, '⚠️ Error building download. Please try again.');

    const { zipPath, stats, currency } = result;
    const caption =
      `📦 ${PERIOD_NAMES[period]}\n` +
      `${stats.count} invoices · ${formatAmount(stats.total, currency)}\n` +
      `✅ Paid: ${formatAmount(stats.paid, currency)}`;

    await waSendDocument(to, zipPath, `InvoKash_${period}.zip`, caption);
    try { fs.unlinkSync(zipPath); } catch (_) {}
  } catch (err) {
    console.error('WA download error:', err.message);
    waSend(to, '⚠️ Error creating download. Please try again.');
  }
}

async function waConfirmDeleteData(to, userId) {
  const count = (invoiceHistory[userId] || []).length;
  await waSendButtons(to,
    `🗑 *Delete All Your Data*\n\n` +
    `This will permanently delete:\n` +
    `• Your business profile\n` +
    `• All ${count} invoice records\n` +
    `• All customer data\n\n` +
    `This CANNOT be undone.`,
    [
      { id: 'confirm_delete', title: '🗑 Yes, Delete All' },
      { id: 'cancel_delete',  title: '❌ Cancel'          },
    ]
  );
}

// ─── New Feature Screens (WhatsApp) ──────────────────────────────────────────
async function waShowAging(to, userId) {
  const report = getAgingReport(userId);
  if (report.count === 0) return waSend(to, '✅ *All invoices paid!* Nothing outstanding. 🎉');

  const currency = report.currency;
  let msg = `⏱ *Invoice Aging*\n─────────────────────\n\n`;
  msg += `💰 Total Outstanding: *${formatAmount(report.totalUnpaid, currency)}*\n`;
  msg += `📄 ${report.count} unpaid invoice${report.count !== 1 ? 's' : ''}\n\n`;

  for (const [, bucket] of Object.entries(report.buckets)) {
    if (bucket.invoices.length === 0) continue;
    msg += `${bucket.emoji} *${bucket.label}*\n`;
    msg += `${formatAmount(bucket.total, currency)} · ${bucket.invoices.length} invoice${bucket.invoices.length !== 1 ? 's' : ''}\n`;
    bucket.invoices.slice(0, 2).forEach(inv => {
      msg += `  • ${inv.invoice_id} — ${inv.customer_name} (${inv.daysOld}d)\n`;
    });
    msg += '\n';
  }

  await waSend(to, msg);
}

async function waShowGoal(to, userId) {
  if (!companyProfiles[userId]) return waSend(to, '⚠️ Type SETUP first.');
  const goal     = getRevenueGoal(userId);
  const profile  = companyProfiles[userId];
  const currency = profile.currency;
  const thisMonth = filterInvoicesByPeriod(invoiceHistory[userId] || [], 'this_month');
  const monthStats = calculateStats(thisMonth, currency);

  let msg = `🎯 *Monthly Revenue Goal*\n─────────────────────\n\n`;
  if (goal) {
    const pct = Math.min(100, Math.round((monthStats.total / goal.monthly) * 100));
    const bar = asciiBar(monthStats.total, goal.monthly, 12);
    msg += `Goal: *${formatAmount(goal.monthly, currency)}/month*\n`;
    msg += `${bar} ${pct}%\n`;
    msg += `Achieved: ${formatAmount(monthStats.total, currency)}\n`;
    msg += `Remaining: ${formatAmount(Math.max(0, goal.monthly - monthStats.total), currency)}\n\n`;
    if (pct >= 100) msg += `🎉 Goal reached! Excellent work!\n\n`;
  } else {
    msg += `No goal set yet.\n\n`;
  }
  msg += `To set a new goal, reply: *GOAL 10000* (replace 10000 with your target)`;

  // Handle "GOAL 10000" command
  await waSend(to, msg);
}

async function waShowExpenses(to, userId) {
  if (!companyProfiles[userId]) return waSend(to, '⚠️ Type SETUP first.');
  const expenses = getExpenses(userId, 'this_month');
  const allExp   = getExpenses(userId, 'all');
  const currency = companyProfiles[userId].currency;
  const monthTotal = expenses.reduce((s, e) => s + (parseFloat(e.amount) || 0), 0);

  if (allExp.length === 0) {
    return waSend(to,
      `💸 *Expense Tracker*\n─────────────────────\n\n` +
      `No expenses logged yet.\n\n` +
      `Log an expense by typing:\n"Spent 500 on petrol"\n"Office supplies 200"\n"Paid 1500 for software"`
    );
  }

  let msg = `💸 *Expenses — This Month*\n─────────────────────\n\n`;
  msg += `Total: *${formatAmount(monthTotal, currency)}*\n\n`;
  expenses.slice(-5).reverse().forEach(exp => {
    const commentStr = exp.comment ? `\n  💬 _${exp.comment}_` : '';
    msg += `• ${exp.description} — ${formatAmount(exp.amount, currency)}\n  🏷 ${exp.category} · ${exp.date}${commentStr}\n\n`;
  });
  msg += `Type *PROFIT* for P&L report.`;

  await waSend(to, msg);
}

async function waShowProfit(to, userId) {
  if (!companyProfiles[userId]) return waSend(to, '⚠️ Type SETUP first.');
  const pl       = calculateProfitLoss(userId, 'this_month');
  const currency = companyProfiles[userId].currency;
  const isProfit = pl.profit >= 0;

  let msg = `📈 *Profit & Loss — This Month*\n─────────────────────\n\n`;
  msg += `💰 Revenue:  *${formatAmount(pl.revenue, currency)}*\n`;
  msg += `💸 Expenses: *${formatAmount(pl.expenses, currency)}*\n`;
  msg += `${isProfit ? '✅' : '🔴'} *${isProfit ? 'Profit' : 'Loss'}: ${formatAmount(Math.abs(pl.profit), currency)}*\n`;
  msg += `📊 Margin: ${pl.margin.toFixed(1)}%\n`;

  if (Object.keys(pl.byCategory).length > 0) {
    msg += `\n🏷 *Expense Breakdown*\n`;
    Object.entries(pl.byCategory).sort((a, b) => b[1] - a[1]).forEach(([cat, amt]) => {
      msg += `• ${cat}: ${formatAmount(amt, currency)}\n`;
    });
  }

  await waSend(to, msg);
}

// ─── Stripe Webhook — Auto-mark invoice paid ──────────────────────────────────
app.post('/stripe-webhook', express.raw({ type: 'application/json' }), async (req, res) => {
  let event;

  // Verify Stripe signature if secret is configured
  if (STRIPE_WEBHOOK_SECRET) {
    let stripe;
    try { stripe = require('stripe')(process.env.STRIPE_SECRET_KEY); } catch (_) {}
    if (stripe) {
      const sig = req.headers['stripe-signature'];
      try {
        event = stripe.webhooks.constructEvent(req.body, sig, STRIPE_WEBHOOK_SECRET);
      } catch (err) {
        console.error('Stripe webhook signature error:', err.message);
        return res.status(400).send(`Webhook Error: ${err.message}`);
      }
    } else {
      event = JSON.parse(req.body);
    }
  } else {
    // No secret configured — parse directly (dev/testing mode)
    try { event = JSON.parse(req.body); } catch { return res.status(400).send('Invalid JSON'); }
  }

  // Handle payment success events
  if (['checkout.session.completed', 'payment_intent.succeeded'].includes(event.type)) {
    const obj = event.data.object;
    const invoiceId = obj.metadata?.invoice_id;

    if (invoiceId) {
      // Find which user owns this invoice
      for (const [userId, invs] of Object.entries(invoiceHistory)) {
        const inv = (invs || []).find(i => i.invoice_id === invoiceId);
        if (inv && inv.status !== 'paid') {
          markInvoicePaid(userId, invoiceId);
          const amount = formatAmount(inv.total, inv.currency);
          console.log(`✅ Stripe auto-paid: ${invoiceId} for ${inv.customer_name} (${amount})`);

          // Notify the business owner via WhatsApp if they are a WA user
          if (waSend && userId.startsWith('wa_')) {
            const phone = userId.replace('wa_', '');
            await waSend(phone,
              `💰 Payment Received!\n\n${inv.customer_name} paid ${amount} via Stripe.\nInvoice ${invoiceId} is now marked as paid ✅`
            ).catch(() => {});
          }
          break;
        }
      }
    }
  }

  res.json({ received: true });
});

// ─── Server Start ─────────────────────────────────────────────────────────────
function startWhatsAppServer() {
  if (!WHATSAPP_TOKEN || !PHONE_ID) {
    console.warn('⚠️  WhatsApp not configured (WHATSAPP_TOKEN / WHATSAPP_PHONE_ID missing) — WhatsApp disabled.');
    return;
  }

  app.listen(PORT, () => {
    console.log(`✅ WhatsApp webhook server running on port ${PORT}`);
    console.log(`   Webhook URL: https://your-domain.com/webhook`);
    console.log(`   Verify token: ${VERIFY_TOKEN}`);
  });
}

module.exports = { startWhatsAppServer, app, waSend };
