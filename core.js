/**
 * InvoKash Core — Shared Business Logic
 * Handles: data persistence, AI extraction, PDF generation, payments, stats
 * Used by: bot.js (Telegram) and whatsapp.js (WhatsApp)
 */

require('dotenv').config();
const axios     = require('axios');
const PDFDocument = require('pdfkit');
const fs        = require('fs');
const path      = require('path');
const archiver  = require('archiver');
const OpenAI    = require('openai');
// Lazily initialize Stripe only when a key is available
let _stripe = null;
function getStripe() {
  if (!_stripe && process.env.STRIPE_SECRET_KEY) {
    _stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);
  }
  return _stripe;
}

// ─── Environment ──────────────────────────────────────────────────────────────
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const OPENAI_API_KEY    = process.env.OPENAI_API_KEY;

// ─── Paths ────────────────────────────────────────────────────────────────────
const BASE_DIR      = __dirname;
const INVOICE_DIR   = path.join(BASE_DIR, 'invoices');
const DATA_DIR      = path.join(BASE_DIR, 'data');
const PROFILES_FILE = path.join(DATA_DIR, 'profiles.json');
const HISTORY_FILE  = path.join(DATA_DIR, 'history.json');
const COUNTER_FILE  = path.join(DATA_DIR, 'counters.json');
const LOGO_DIR      = path.join(BASE_DIR, 'logos');

[LOGO_DIR, INVOICE_DIR, DATA_DIR, '/tmp/voice'].forEach(dir => {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

// ─── In-Memory State (shared across platforms) ────────────────────────────────
const companyProfiles = {};
const invoiceHistory  = {};
const invoiceCounters = {};   // { userId: lastNumber }
const onboardingState = {};   // { userId: { step, ... } }
const commandState    = {};   // { userId: { type } }
const pendingInvoices = {};   // { userId: invoiceData }
const userRateLimits  = new Map();

// Simple AI result cache (saves cost by not re-classifying same text)
const intentCache = new Map();

// ─── New Feature State (Goals, Templates, Expenses) ───────────────────────────
const revenueGoals     = {}; // { userId: { monthly: number, currency: string } }
const invoiceTemplates = {}; // { userId: [{ name, line_items, description, amount }] }
const expenseHistory   = {}; // { userId: [{ id, date, amount, description, category }] }

const GOALS_FILE     = path.join(DATA_DIR, 'goals.json');
const TEMPLATES_FILE = path.join(DATA_DIR, 'templates.json');
const EXPENSES_FILE  = path.join(DATA_DIR, 'expenses.json');

// ─── OpenAI Client ────────────────────────────────────────────────────────────
const openai = OPENAI_API_KEY ? new OpenAI({ apiKey: OPENAI_API_KEY }) : null;

// ─── Currency Config ──────────────────────────────────────────────────────────
const CURRENCIES = {
  AED: { symbol: 'AED', flag: '🇦🇪', name: 'UAE Dirham',       tax: 'VAT', right: true  },
  USD: { symbol: '$',   flag: '🇺🇸', name: 'US Dollar',         tax: 'VAT', right: false },
  EUR: { symbol: '€',   flag: '🇪🇺', name: 'Euro',              tax: 'VAT', right: false },
  GBP: { symbol: '£',   flag: '🇬🇧', name: 'British Pound',     tax: 'VAT', right: false },
  INR: { symbol: '₹',   flag: '🇮🇳', name: 'Indian Rupee',      tax: 'GST', right: false },
  SAR: { symbol: 'SAR', flag: '🇸🇦', name: 'Saudi Riyal',       tax: 'VAT', right: true  },
  OMR: { symbol: 'OMR', flag: '🇴🇲', name: 'Omani Rial',        tax: 'VAT', right: true  },
  KWD: { symbol: 'KWD', flag: '🇰🇼', name: 'Kuwaiti Dinar',     tax: 'VAT', right: true  },
  BHD: { symbol: 'BHD', flag: '🇧🇭', name: 'Bahraini Dinar',    tax: 'VAT', right: true  },
  QAR: { symbol: 'QAR', flag: '🇶🇦', name: 'Qatari Riyal',      tax: 'VAT', right: true  },
  EGP: { symbol: 'EGP', flag: '🇪🇬', name: 'Egyptian Pound',    tax: 'VAT', right: true  },
  SGD: { symbol: 'S$',  flag: '🇸🇬', name: 'Singapore Dollar',  tax: 'GST', right: false },
  CAD: { symbol: 'CA$', flag: '🇨🇦', name: 'Canadian Dollar',   tax: 'VAT', right: false },
  AUD: { symbol: 'A$',  flag: '🇦🇺', name: 'Australian Dollar', tax: 'GST', right: false },
};

const PERIOD_NAMES = {
  this_month:   'This Month',
  last_month:   'Last Month',
  this_quarter: 'This Quarter',
  this_year:    'This Year',
  all:          'All Time',
};

const EXPENSE_CATEGORIES = ['Travel', 'Software', 'Office', 'Marketing', 'Subcontractors', 'Equipment', 'Other'];

// ─── Data Persistence ─────────────────────────────────────────────────────────
function loadData() {
  try {
    if (fs.existsSync(PROFILES_FILE)) Object.assign(companyProfiles,  JSON.parse(fs.readFileSync(PROFILES_FILE,  'utf8')));
    if (fs.existsSync(HISTORY_FILE))  Object.assign(invoiceHistory,   JSON.parse(fs.readFileSync(HISTORY_FILE,   'utf8')));
    if (fs.existsSync(COUNTER_FILE))  Object.assign(invoiceCounters,  JSON.parse(fs.readFileSync(COUNTER_FILE,   'utf8')));
    if (fs.existsSync(GOALS_FILE))    Object.assign(revenueGoals,     JSON.parse(fs.readFileSync(GOALS_FILE,     'utf8')));
    if (fs.existsSync(TEMPLATES_FILE))Object.assign(invoiceTemplates, JSON.parse(fs.readFileSync(TEMPLATES_FILE, 'utf8')));
    if (fs.existsSync(EXPENSES_FILE)) Object.assign(expenseHistory,   JSON.parse(fs.readFileSync(EXPENSES_FILE,  'utf8')));
  } catch (err) { console.error('Load error:', err.message); }
}

function saveData() {
  try {
    fs.writeFileSync(PROFILES_FILE,  JSON.stringify(companyProfiles,  null, 2));
    fs.writeFileSync(HISTORY_FILE,   JSON.stringify(invoiceHistory,   null, 2));
    fs.writeFileSync(COUNTER_FILE,   JSON.stringify(invoiceCounters,  null, 2));
    fs.writeFileSync(GOALS_FILE,     JSON.stringify(revenueGoals,     null, 2));
    fs.writeFileSync(TEMPLATES_FILE, JSON.stringify(invoiceTemplates, null, 2));
    fs.writeFileSync(EXPENSES_FILE,  JSON.stringify(expenseHistory,   null, 2));
  } catch (err) { console.error('Save error:', err.message); }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────
function checkRateLimit(userId) {
  const now   = Date.now();
  const limit = userRateLimits.get(userId) || { count: 0, resetTime: now + 60000 };
  if (now > limit.resetTime) { limit.count = 0; limit.resetTime = now + 60000; }
  limit.count++;
  userRateLimits.set(userId, limit);
  return limit.count <= 25;
}

function sanitizeInput(input) {
  if (typeof input !== 'string') return '';
  return input.replace(/[<>]/g, '').trim().slice(0, 800);
}

function formatAmount(amount, currency) {
  const cfg = CURRENCIES[currency];
  const num = parseFloat(amount || 0).toFixed(2);
  if (!cfg) return `${currency} ${num}`;
  return cfg.right ? `${num} ${cfg.symbol}` : `${cfg.symbol}${num}`;
}

function progressBar(step, total) {
  const filled = Math.round((step / total) * 10);
  return `[${'█'.repeat(filled)}${'░'.repeat(10 - filled)}] ${step}/${total}`;
}

function getTaxConfig(profile) {
  if (profile.currency === 'INR' || profile.currency === 'SGD' || profile.currency === 'AUD') {
    return { enabled: !!profile.gst_enabled, rate: profile.gst_rate || 0, type: 'GST', field: 'gst' };
  }
  return { enabled: !!profile.vat_enabled, rate: profile.vat_rate || 0, type: 'VAT', field: 'vat' };
}

function generateInvoiceId(userId) {
  const year = new Date().getFullYear();
  if (!invoiceCounters[userId]) invoiceCounters[userId] = 0;
  invoiceCounters[userId]++;
  const num = String(invoiceCounters[userId]).padStart(4, '0');
  return `INV-${year}-${num}`;
}

function asciiBar(value, max, width = 12) {
  const filled = max > 0 ? Math.round((value / max) * width) : 0;
  return `[${'█'.repeat(filled)}${'░'.repeat(width - filled)}]`;
}

// ─── Invoice Filtering ────────────────────────────────────────────────────────
function filterInvoicesByPeriod(invoices, period) {
  const now = new Date();
  return invoices.filter(inv => {
    const parts = inv.date?.split('/');
    if (!parts || parts.length < 3) return false;
    const d = new Date(parseInt(parts[2]), parseInt(parts[1]) - 1, parseInt(parts[0]));
    if (isNaN(d)) return false;
    if (period === 'this_month')   return d.getMonth() === now.getMonth() && d.getFullYear() === now.getFullYear();
    if (period === 'last_month') {
      const last = new Date(now.getFullYear(), now.getMonth() - 1, 1);
      return d.getMonth() === last.getMonth() && d.getFullYear() === last.getFullYear();
    }
    if (period === 'this_quarter') return Math.floor(d.getMonth() / 3) === Math.floor(now.getMonth() / 3) && d.getFullYear() === now.getFullYear();
    if (period === 'this_year')    return d.getFullYear() === now.getFullYear();
    return true;
  });
}

// ─── Stats Calculation ────────────────────────────────────────────────────────
function calculateStats(invoices, currency) {
  const total    = invoices.reduce((s, i) => s + (parseFloat(i.total) || 0), 0);
  const taxTotal = invoices.reduce((s, i) => s + (parseFloat(i.tax_amount) || 0), 0);
  const subtotal = total - taxTotal;
  const avg      = invoices.length > 0 ? total / invoices.length : 0;
  const paid     = invoices.filter(i => i.status === 'paid').reduce((s, i) => s + (parseFloat(i.total) || 0), 0);
  const unpaid   = total - paid;

  const custTotals = {};
  invoices.forEach(inv => {
    const name = inv.customer_name?.trim();
    if (name) custTotals[name] = (custTotals[name] || 0) + (parseFloat(inv.total) || 0);
  });
  const topCustomers = Object.entries(custTotals).sort((a, b) => b[1] - a[1]).slice(0, 5);

  return { total, subtotal, taxTotal, avg, paid, unpaid, topCustomers, count: invoices.length };
}

// ─── CSV Generation ───────────────────────────────────────────────────────────
function generateCSV(invoices) {
  const esc = (v) => {
    if (v == null) return '';
    const s = String(v).replace(/"/g, '""');
    return (s.includes(',') || s.includes('"') || s.includes('\n')) ? `"${s}"` : s;
  };
  let csv = 'Invoice ID,Date,Customer,Service,Subtotal,Tax,Total,Currency,Status,Payment Link\n';
  invoices.forEach(inv => {
    const sub = ((parseFloat(inv.total) || 0) - (parseFloat(inv.tax_amount) || 0)).toFixed(2);
    csv += [
      esc(inv.invoice_id), esc(inv.date), esc(inv.customer_name),
      esc(inv.service), esc(sub), esc(inv.tax_amount || '0.00'),
      esc(inv.total), esc(inv.currency), esc(inv.status || 'pending'),
      esc(inv.payment_link || '')
    ].join(',') + '\n';
  });
  return csv;
}

// ─── AI: Intent Classification (with caching for cost savings) ────────────────
// Simple regex pre-screen — avoids AI call entirely for obvious cases
function quickClassify(text) {
  const t = text.toLowerCase().trim();
  if (/^(hi|hello|hey|good (morning|evening|afternoon)|howdy|greetings|salaam|مرحبا)/i.test(t)) return 'greeting';
  if (/^\/?(help|commands?|how|what can)/i.test(t)) return 'help';
  if (/\b(download|export|csv|zip)\b/i.test(t)) return 'download';
  if (/\b(stat(s|istic)?s?|revenue|earnings?|report)\b/i.test(t)) return 'stats';
  if (/\b(invoice|bill|invoices|history)\b/i.test(t) && !/\bfor\b.*\d/.test(t)) return 'list_invoices';
  if (/\b(profile|settings?|account)\b/i.test(t)) return 'profile';
  if (/\b(customer|client)s?\b/i.test(t) && !/\bfor\b.*\d/.test(t)) return 'customers';
  // Invoice pattern: has "for X" with a number
  if (/for\s+\S+.*\s+for\s+[\d,]+(\.\d+)?/i.test(t)) return 'invoice';
  if (/\d+(\.\d+)?\s*(aed|usd|eur|gbp|inr|sar|omr|kwd|qar|bhd|sgd|cad|aud|egp|دولار|درهم)/i.test(t)) return 'invoice';
  return null; // needs AI
}

async function classifyIntent(text) {
  const quick = quickClassify(text);
  if (quick) return quick;

  const cacheKey = text.toLowerCase().trim().slice(0, 100);
  if (intentCache.has(cacheKey)) return intentCache.get(cacheKey);

  try {
    const res = await axios.post('https://api.anthropic.com/v1/messages',
      {
        model:      'claude-haiku-4-5-20251001',
        max_tokens: 10,
        messages:   [{ role: 'user', content: `Classify this message as exactly ONE word — invoice, greeting, help, stats, download, invalid:\n"${text.slice(0, 200)}"\nAnswer:` }]
      },
      { headers: { 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' }, timeout: 10000 }
    );
    const intent = res.data.content[0].text.toLowerCase().trim().replace(/[^a-z_]/g, '');
    const valid  = ['invoice','greeting','help','stats','download','invalid'].includes(intent) ? intent : 'invalid';
    intentCache.set(cacheKey, valid);
    if (intentCache.size > 500) intentCache.delete(intentCache.keys().next().value); // LRU eviction
    return valid;
  } catch (err) {
    console.error('Intent classify error:', err.message);
    return 'invalid';
  }
}

// ─── AI: Extract Invoice Data (Haiku for cost efficiency) ─────────────────────
async function extractInvoiceData(text) {
  const res = await axios.post('https://api.anthropic.com/v1/messages',
    {
      model:      'claude-haiku-4-5-20251001',
      max_tokens: 512,
      messages:   [{
        role:    'user',
        content: `Extract invoice details from this text. Return ONLY valid JSON, nothing else.

Text: "${text.slice(0, 600)}"

Return this exact JSON:
{
  "customer_name": "name of person/company being billed",
  "address": "location if mentioned, or null",
  "line_items": [
    { "description": "service or item name", "amount": 0.00 }
  ]
}

Rules: customer_name = who is billed, amounts are numbers only (no currency symbols), split multi-service invoices into separate line_items.`
      }]
    },
    { headers: { 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' }, timeout: 20000 }
  );

  let raw = res.data.content[0].text.replace(/```json\n?|\n?```/g, '').trim();
  const m = raw.match(/\{[\s\S]*\}/);
  if (m) raw = m[0];
  return JSON.parse(raw);
}

function validateInvoiceData(data) {
  const errors = [];
  if (!data.customer_name?.trim())   errors.push('Customer name');
  if (!data.line_items?.length)      errors.push('Service description');
  const total = (data.line_items || []).reduce((s, i) => s + (parseFloat(i.amount) || 0), 0);
  if (total <= 0)                    errors.push('Amount greater than 0');
  return { valid: errors.length === 0, errors };
}

// ─── Voice Transcription ──────────────────────────────────────────────────────
async function transcribeAudio(filePath) {
  if (!openai) throw new Error('OpenAI not configured');
  const transcription = await openai.audio.transcriptions.create({
    file:  fs.createReadStream(filePath),
    model: 'whisper-1',
  });
  return transcription.text;
}

// ─── Payment Link ─────────────────────────────────────────────────────────────
async function createPaymentLink(invoiceData) {
  const stripe = getStripe();
  if (!stripe) return { success: false, error: 'Stripe not configured' };
  try {
    // Stripe doesn't support all currencies for payment links — map unsupported ones
    const stripeSupportedCurrencies = ['usd','eur','gbp','inr','cad','aud','sgd','aed','sar','qar','bhd','omr','kwd','egp'];
    const currency = invoiceData.currency.toLowerCase();
    if (!stripeSupportedCurrencies.includes(currency)) {
      return { success: false, error: `Currency ${invoiceData.currency} not supported by Stripe` };
    }

    const paymentLink = await stripe.paymentLinks.create({
      line_items: [{
        price_data: {
          currency,
          product_data: {
            name:        `Invoice ${invoiceData.invoice_id}`,
            description: `Payment for services rendered to ${invoiceData.customer_name}`,
          },
          unit_amount: Math.round(parseFloat(invoiceData.total) * 100),
        },
        quantity: 1,
      }],
      metadata: { invoice_id: invoiceData.invoice_id, customer_name: invoiceData.customer_name },
      after_completion: { type: 'hosted_confirmation', hosted_confirmation: { custom_message: `Thank you! Invoice ${invoiceData.invoice_id} has been paid.` } },
    });
    return { success: true, paymentUrl: paymentLink.url, linkId: paymentLink.id };
  } catch (err) {
    console.error('Stripe error:', err.message);
    return { success: false, error: err.message };
  }
}

// ─── PDF Generation ───────────────────────────────────────────────────────────
async function generateProfessionalInvoice(data) {
  return new Promise(async (resolve, reject) => {
    const pdfPath = `/tmp/invoice_${Date.now()}_${Math.random().toString(36).slice(2)}.pdf`;
    const doc     = new PDFDocument({ margin: 0, size: 'A4', info: { Title: `Invoice ${data.invoice_id}`, Author: data.company_name } });
    const stream  = fs.createWriteStream(pdfPath);
    doc.pipe(stream);

    const W        = 595.28;
    const H        = 841.89;
    const MARGIN   = 45;
    const INNER_W  = W - MARGIN * 2;

    // ── Brand Colors ──────────────────────────────────────────────────────────
    const NAVY     = '#0F172A';    // Dark navy (header/footer)
    const ACCENT   = '#6366F1';    // Indigo (brand accent)
    const ACCENT2  = '#818CF8';    // Lighter indigo
    const LIGHT_BG = '#F8FAFF';    // Near-white blue tint
    const BORDER   = '#E2E8F0';    // Subtle border color
    const MUTED    = '#64748B';    // Muted text
    const DARK     = '#0F172A';    // Primary text
    const WHITE    = '#FFFFFF';
    const PAID_GRN = '#10B981';    // Emerald green (paid badge)

    const curr   = CURRENCIES[data.currency] || { symbol: data.currency };
    const fmtAmt = (v) => {
      const n = parseFloat(v || 0).toFixed(2);
      return curr.right ? `${n} ${curr.symbol}` : `${curr.symbol}${n}`;
    };

    // ── Header Band ───────────────────────────────────────────────────────────
    doc.rect(0, 0, W, 100).fill(NAVY);
    // Subtle gradient effect via layered rect
    doc.rect(0, 0, W, 50).fillOpacity(0.15).fill(ACCENT).fillOpacity(1);

    // Logo
    let logoEndX = MARGIN;
    if (data.logo_path && fs.existsSync(data.logo_path)) {
      try {
        doc.image(data.logo_path, MARGIN, 14, { width: 56, height: 56, fit: [56, 56] });
        logoEndX = MARGIN + 68;
      } catch (_) {}
    }

    // Company name & address in header
    doc.fillColor(WHITE).font('Helvetica-Bold').fontSize(16)
       .text(data.company_name || '', logoEndX, 20, { width: 260 });
    doc.font('Helvetica').fontSize(8).fillColor(ACCENT2)
       .text(data.company_address || '', logoEndX, 42, { width: 260 });
    if (data.trn) {
      doc.text(`TRN: ${data.trn}`, logoEndX, 56, { width: 260 });
    }

    // INVOICE label (right side)
    doc.fillColor(ACCENT2).font('Helvetica-Bold').fontSize(28)
       .text('INVOICE', 340, 18, { align: 'right', width: W - 340 - MARGIN });

    // Invoice ID & date under label
    doc.font('Helvetica').fontSize(8).fillColor(WHITE)
       .text(data.invoice_id, 340, 56, { align: 'right', width: W - 340 - MARGIN });
    doc.fillColor(ACCENT2)
       .text(data.date, 340, 68, { align: 'right', width: W - 340 - MARGIN });

    // Paid badge (if applicable)
    if (data.status === 'paid') {
      doc.roundedRect(W - MARGIN - 62, 75, 62, 18, 9).fill(PAID_GRN);
      doc.font('Helvetica-Bold').fontSize(8).fillColor(WHITE)
         .text('✓ PAID', W - MARGIN - 56, 80, { width: 56, align: 'center' });
    }

    // ── Meta Row (Invoice # + Date in info cards) ─────────────────────────────
    let y = 112;
    doc.roundedRect(MARGIN, y, INNER_W, 48, 8).fill(LIGHT_BG);

    // Invoice number card
    doc.font('Helvetica-Bold').fontSize(7).fillColor(MUTED)
       .text('INVOICE NUMBER', MARGIN + 16, y + 10);
    doc.font('Helvetica-Bold').fontSize(13).fillColor(DARK)
       .text(data.invoice_id, MARGIN + 16, y + 22);

    // Date card
    doc.font('Helvetica-Bold').fontSize(7).fillColor(MUTED)
       .text('ISSUE DATE', MARGIN + 200, y + 10);
    doc.font('Helvetica-Bold').fontSize(13).fillColor(DARK)
       .text(data.date, MARGIN + 200, y + 22);

    // Due Date (30 days from issue)
    const dueDate = new Date();
    dueDate.setDate(dueDate.getDate() + 30);
    const dueDateStr = dueDate.toLocaleDateString('en-GB');
    doc.font('Helvetica-Bold').fontSize(7).fillColor(MUTED)
       .text('DUE DATE', MARGIN + 350, y + 10);
    doc.font('Helvetica-Bold').fontSize(13).fillColor(DARK)
       .text(dueDateStr, MARGIN + 350, y + 22);

    // ── Bill To + From Cards ───────────────────────────────────────────────────
    y += 62;
    const cardH = 76;
    const halfW = (INNER_W - 10) / 2;

    // Bill To card
    doc.roundedRect(MARGIN, y, halfW, cardH, 8).fill(LIGHT_BG);
    doc.font('Helvetica-Bold').fontSize(7).fillColor(MUTED)
       .text('BILL TO', MARGIN + 16, y + 12);
    doc.font('Helvetica-Bold').fontSize(12).fillColor(DARK)
       .text(data.customer_name || '', MARGIN + 16, y + 26, { width: halfW - 32 });
    if (data.address && data.address !== 'null' && data.address?.trim()) {
      doc.font('Helvetica').fontSize(9).fillColor(MUTED)
         .text(data.address, MARGIN + 16, y + 46, { width: halfW - 32 });
    }

    // From card
    const fromX = MARGIN + halfW + 10;
    doc.roundedRect(fromX, y, halfW, cardH, 8).fill(LIGHT_BG);
    doc.font('Helvetica-Bold').fontSize(7).fillColor(MUTED)
       .text('FROM', fromX + 16, y + 12);
    doc.font('Helvetica-Bold').fontSize(12).fillColor(DARK)
       .text(data.company_name || '', fromX + 16, y + 26, { width: halfW - 32 });
    doc.font('Helvetica').fontSize(9).fillColor(MUTED)
       .text(data.company_address || '', fromX + 16, y + 46, { width: halfW - 32 });

    // ── Line Items Table ───────────────────────────────────────────────────────
    y += cardH + 20;

    // Table header
    doc.roundedRect(MARGIN, y, INNER_W, 28, 4).fill(NAVY);
    doc.font('Helvetica-Bold').fontSize(8.5).fillColor(WHITE)
       .text('DESCRIPTION', MARGIN + 16, y + 9)
       .text('QTY', MARGIN + INNER_W - 155, y + 9, { align: 'right', width: 30 })
       .text('UNIT PRICE', MARGIN + INNER_W - 120, y + 9, { align: 'right', width: 70 })
       .text('AMOUNT', MARGIN + INNER_W - 45, y + 9, { align: 'right', width: 45 });
    y += 28;

    let rowNum = 0;
    (data.line_items || []).forEach(item => {
      if (y > 700) { doc.addPage({ margin: 0, size: 'A4' }); y = 40; }
      const rowBg = rowNum % 2 === 0 ? WHITE : '#F1F5FF';
      const rowH  = 30;
      doc.rect(MARGIN, y, INNER_W, rowH).fill(rowBg);

      // Left border accent on first item
      if (rowNum === 0) {
        doc.rect(MARGIN, y, 3, rowH).fill(ACCENT);
      }

      doc.font('Helvetica').fontSize(10).fillColor(DARK)
         .text(item.description || '', MARGIN + 16, y + 10, { width: INNER_W - 200 });
      doc.text('1', MARGIN + INNER_W - 155, y + 10, { align: 'right', width: 30 });
      doc.text(fmtAmt(item.amount), MARGIN + INNER_W - 120, y + 10, { align: 'right', width: 70 });
      doc.font('Helvetica-Bold').fillColor(DARK)
         .text(fmtAmt(item.amount), MARGIN + INNER_W - 45, y + 10, { align: 'right', width: 45 });

      y += rowH;
      rowNum++;
    });

    // Bottom border of table
    doc.moveTo(MARGIN, y).lineTo(MARGIN + INNER_W, y).strokeColor(BORDER).lineWidth(1).stroke();
    y += 16;

    // ── Totals Section ────────────────────────────────────────────────────────
    if (y > 680) { doc.addPage({ margin: 0, size: 'A4' }); y = 40; }

    const totalsX = W - MARGIN - 230;
    const totalsW = 230;

    // Subtotal row
    doc.font('Helvetica').fontSize(10).fillColor(MUTED)
       .text('Subtotal', totalsX, y)
       .text(fmtAmt(data.subtotal), totalsX, y, { align: 'right', width: totalsW });
    y += 22;

    // Tax row
    if (data.tax_enabled && parseFloat(data.tax_amount) > 0) {
      doc.text(`${data.tax_type} (${data.tax_rate}%)`, totalsX, y)
         .text(fmtAmt(data.tax_amount), totalsX, y, { align: 'right', width: totalsW });
      y += 22;
    }

    // Divider
    doc.moveTo(totalsX, y).lineTo(W - MARGIN, y).strokeColor(BORDER).lineWidth(1).stroke();
    y += 10;

    // Total box
    if (y > 720) { doc.addPage({ margin: 0, size: 'A4' }); y = 40; }
    doc.roundedRect(totalsX, y, totalsW, 40, 8).fill(ACCENT);
    doc.font('Helvetica-Bold').fontSize(10).fillColor(WHITE)
       .text('TOTAL DUE', totalsX + 16, y + 13)
       .text(fmtAmt(data.total), totalsX, y + 13, { align: 'right', width: totalsW - 16 });
    y += 56;

    // ── Payment Details ────────────────────────────────────────────────────────
    if (y > 680) { doc.addPage({ margin: 0, size: 'A4' }); y = 40; }

    const bankH = data.payment_link ? 84 : 72;
    doc.roundedRect(MARGIN, y, INNER_W, bankH, 8).fill(LIGHT_BG);
    doc.moveTo(MARGIN, y).lineTo(MARGIN + 4, y).stroke(); // accent strip
    doc.rect(MARGIN, y, 4, bankH).fill(ACCENT);

    doc.font('Helvetica-Bold').fontSize(7.5).fillColor(MUTED)
       .text('PAYMENT DETAILS', MARGIN + 16, y + 12);
    doc.font('Helvetica').fontSize(9.5).fillColor(DARK)
       .text(`Bank: ${data.bank_name || 'N/A'}`,       MARGIN + 16, y + 26)
       .text(`IBAN / Account: ${data.iban || 'N/A'}`,  MARGIN + 16, y + 42)
       .text(`Account Name: ${data.account_name || 'N/A'}`, MARGIN + 16, y + 56);

    if (data.payment_link) {
      doc.font('Helvetica-Bold').fontSize(8.5).fillColor(ACCENT)
         .text(`Online Payment: ${data.payment_link}`, MARGIN + 16, y + 72, { width: INNER_W - 32 });
    }

    y += bankH + 16;

    // ── Notes / Terms ─────────────────────────────────────────────────────────
    if (data.notes) {
      doc.roundedRect(MARGIN, y, INNER_W, 50, 8).fill(LIGHT_BG);
      doc.font('Helvetica-Bold').fontSize(7.5).fillColor(MUTED).text('NOTES', MARGIN + 16, y + 12);
      doc.font('Helvetica').fontSize(9).fillColor(MUTED).text(data.notes, MARGIN + 16, y + 26, { width: INNER_W - 32 });
      y += 66;
    }

    // ── Footer ────────────────────────────────────────────────────────────────
    doc.rect(0, H - 36, W, 36).fill(NAVY);
    doc.font('Helvetica').fontSize(7.5).fillColor(ACCENT2)
       .text(`Generated by InvoKash  ·  For record-keeping purposes only  ·  Not a legally certified tax document  ·  ${data.invoice_id}`,
             MARGIN, H - 22, { align: 'center', width: INNER_W });

    doc.end();
    stream.on('finish', () => resolve(pdfPath));
    stream.on('error', reject);
  });
}

// ─── Core Invoice Processing (shared between Telegram & WhatsApp) ──────────────
async function processInvoiceText(userId, text) {
  const profile = companyProfiles[userId];
  if (!profile) return { error: 'no_profile' };

  let data;
  try {
    data = await extractInvoiceData(text);
  } catch (err) {
    return { error: 'parse_failed' };
  }

  const validation = validateInvoiceData(data);
  if (!validation.valid) return { error: 'validation', errors: validation.errors };

  const tc       = getTaxConfig(profile);
  const subtotal = data.line_items.reduce((s, i) => s + (parseFloat(i.amount) || 0), 0);
  const tax      = tc.enabled ? subtotal * (tc.rate / 100) : 0;
  const total    = subtotal + tax;

  const pending = { data, profile, subtotal, tax, total, tc };
  pendingInvoices[userId] = pending;
  return { success: true, pending };
}

async function confirmInvoice(userId) {
  const pending = pendingInvoices[userId];
  if (!pending) return { error: 'no_pending' };
  delete pendingInvoices[userId];

  const { data, profile, subtotal, tax, total, tc } = pending;
  const invoiceId = generateInvoiceId(userId);
  const date      = new Date().toLocaleDateString('en-GB');

  const fullData = {
    customer_name: data.customer_name, address: data.address,
    company_name: profile.company_name, company_address: profile.company_address,
    trn: profile.trn, currency: profile.currency,
    bank_name: profile.bank_name, iban: profile.iban, account_name: profile.account_name,
    tax_enabled: tc.enabled, tax_rate: tc.rate, tax_type: tc.type,
    logo_path: profile.logo_path, invoice_id: invoiceId, date,
    line_items: data.line_items,
    subtotal: subtotal.toFixed(2), tax_amount: tax.toFixed(2), total: total.toFixed(2),
    status: 'pending',
  };

  const pdfPath = await generateProfessionalInvoice(fullData);
  const permanentPath = path.join(INVOICE_DIR, `${userId}_${invoiceId}.pdf`);
  fs.copyFileSync(pdfPath, permanentPath);

  if (!invoiceHistory[userId]) invoiceHistory[userId] = [];
  const record = {
    invoice_id: invoiceId, customer_name: data.customer_name,
    service: data.line_items.map(i => i.description).join(', '),
    total: total.toFixed(2), tax_amount: tax.toFixed(2),
    currency: profile.currency, date, file_path: permanentPath, status: 'pending',
  };
  invoiceHistory[userId].push(record);
  saveData();

  const paymentResult = await createPaymentLink({
    invoice_id: invoiceId, customer_name: data.customer_name,
    total: total.toFixed(2), currency: profile.currency,
  });

  if (paymentResult.success) {
    record.payment_link = paymentResult.paymentUrl;
    saveData();
  }

  return {
    success: true, pdfPath, invoiceId,
    customer: data.customer_name,
    total: total.toFixed(2), currency: profile.currency,
    paymentUrl: paymentResult.success ? paymentResult.paymentUrl : null,
  };
}

// ─── Mark Invoice Paid ────────────────────────────────────────────────────────
function markInvoicePaid(userId, invoiceId) {
  const invs = invoiceHistory[userId] || [];
  const inv  = invs.find(i => i.invoice_id === invoiceId);
  if (!inv) return false;
  inv.status = 'paid';
  saveData();
  return true;
}

// ─── Download ZIP ─────────────────────────────────────────────────────────────
async function buildDownloadZip(userId, period) {
  const invs     = invoiceHistory[userId] || [];
  const filtered = filterInvoicesByPeriod(invs, period);
  if (filtered.length === 0) return null;

  const ts      = Date.now();
  const zipPath = `/tmp/invoices_${userId}_${ts}.zip`;
  const csvPath = `/tmp/invoices_${userId}_${ts}.csv`;
  const currency = companyProfiles[userId]?.currency || 'AED';

  fs.writeFileSync(csvPath, generateCSV(filtered));

  await new Promise((resolve, reject) => {
    const output  = fs.createWriteStream(zipPath);
    const archive = archiver('zip', { zlib: { level: 7 } });
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

  try { fs.unlinkSync(csvPath); } catch (_) {}

  const stats = calculateStats(filtered, currency);
  return { zipPath, filtered, stats, currency };
}

// ─── Quick Re-Invoice ─────────────────────────────────────────────────────────
function getLastInvoiceForCustomer(userId, customerName) {
  const invs       = invoiceHistory[userId] || [];
  const normalized = customerName.toLowerCase().trim();
  const matches    = invs.filter(inv => {
    const name = inv.customer_name?.toLowerCase().trim() || '';
    return name.includes(normalized) || normalized.includes(name);
  });
  return matches[matches.length - 1] || null;
}

// ─── Invoice Aging Dashboard ──────────────────────────────────────────────────
function getAgingReport(userId) {
  const invs     = (invoiceHistory[userId] || []).filter(i => i.status !== 'paid');
  const currency = companyProfiles[userId]?.currency || 'AED';
  const now      = new Date();

  const buckets = {
    current: { label: 'Current (0–30 days)',  emoji: '🟢', invoices: [], total: 0 },
    days30:  { label: '31–60 Days',           emoji: '🟡', invoices: [], total: 0 },
    days60:  { label: '61–90 Days',           emoji: '🟠', invoices: [], total: 0 },
    days90:  { label: '90+ Days (Critical)',  emoji: '🔴', invoices: [], total: 0 },
  };

  invs.forEach(inv => {
    const parts = inv.date?.split('/');
    if (!parts || parts.length < 3) return;
    const d       = new Date(parseInt(parts[2]), parseInt(parts[1]) - 1, parseInt(parts[0]));
    if (isNaN(d)) return;
    const daysOld = Math.floor((now - d) / (1000 * 60 * 60 * 24));
    const amount  = parseFloat(inv.total) || 0;

    let key;
    if (daysOld <= 30)      key = 'current';
    else if (daysOld <= 60) key = 'days30';
    else if (daysOld <= 90) key = 'days60';
    else                    key = 'days90';

    buckets[key].invoices.push({ ...inv, daysOld });
    buckets[key].total += amount;
  });

  const totalUnpaid = invs.reduce((s, i) => s + (parseFloat(i.total) || 0), 0);
  return { buckets, currency, totalUnpaid, count: invs.length };
}

// ─── Revenue Goals ────────────────────────────────────────────────────────────
function setRevenueGoal(userId, monthlyGoal) {
  revenueGoals[userId] = {
    monthly:  parseFloat(monthlyGoal),
    currency: companyProfiles[userId]?.currency || 'AED',
    setAt:    new Date().toISOString(),
  };
  saveData();
}

function getRevenueGoal(userId) {
  return revenueGoals[userId] || null;
}

// ─── AI Business Insights ─────────────────────────────────────────────────────
async function generateBusinessInsights(userId, stats, period) {
  const profile = companyProfiles[userId];
  if (!profile) return null;
  const currency = profile.currency;

  try {
    const paidPct = stats.total > 0 ? Math.round((stats.paid / stats.total) * 100) : 0;
    const prompt  =
      `You are a sharp business advisor. Given these stats, write exactly 2 sentences of specific, actionable insight for a freelancer/SMB. Be direct and encouraging. Reference actual numbers.\n\n` +
      `Business: ${profile.company_name}\nPeriod: ${period}\n` +
      `Revenue: ${formatAmount(stats.total, currency)} across ${stats.count} invoices\n` +
      `Paid: ${formatAmount(stats.paid, currency)} (${paidPct}% collection rate)\n` +
      `Outstanding: ${formatAmount(stats.unpaid, currency)}\n` +
      `Average invoice: ${formatAmount(stats.avg, currency)}\n` +
      `Top client: ${stats.topCustomers?.[0]?.[0] || 'N/A'}\n\n` +
      `Write exactly 2 sentences. Start with a key observation, end with one concrete action.`;

    const res = await axios.post('https://api.anthropic.com/v1/messages',
      { model: 'claude-haiku-4-5-20251001', max_tokens: 120, messages: [{ role: 'user', content: prompt }] },
      { headers: { 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' }, timeout: 12000 }
    );
    return res.data.content[0].text.trim();
  } catch (err) {
    console.error('AI insights error:', err.message);
    return null;
  }
}

// ─── Client Statement PDF ─────────────────────────────────────────────────────
async function generateClientStatement(userId, customerName) {
  const profile = companyProfiles[userId];
  if (!profile) return null;

  const invs = (invoiceHistory[userId] || []).filter(inv =>
    inv.customer_name?.toLowerCase().includes(customerName.toLowerCase())
  );
  if (invs.length === 0) return null;

  return new Promise((resolve, reject) => {
    const pdfPath = `/tmp/statement_${Date.now()}.pdf`;
    const doc     = new PDFDocument({ margin: 0, size: 'A4' });
    const stream  = fs.createWriteStream(pdfPath);
    doc.pipe(stream);

    const W       = 595.28;
    const H       = 841.89;
    const MARGIN  = 45;
    const INNER_W = W - MARGIN * 2;

    const NAVY     = '#0F172A';
    const ACCENT   = '#6366F1';
    const ACCENT2  = '#818CF8';
    const LIGHT_BG = '#F8FAFF';
    const BORDER   = '#E2E8F0';
    const MUTED    = '#64748B';
    const DARK     = '#0F172A';
    const WHITE    = '#FFFFFF';
    const GREEN    = '#10B981';
    const RED      = '#EF4444';

    const curr   = CURRENCIES[profile.currency] || { symbol: profile.currency };
    const fmtAmt = (v) => { const n = parseFloat(v || 0).toFixed(2); return curr.right ? `${n} ${curr.symbol}` : `${curr.symbol}${n}`; };

    // Header
    doc.rect(0, 0, W, 100).fill(NAVY);
    doc.rect(0, 0, W, 50).fillOpacity(0.15).fill(ACCENT).fillOpacity(1);
    doc.fillColor(WHITE).font('Helvetica-Bold').fontSize(16).text(profile.company_name || '', MARGIN, 20, { width: 260 });
    doc.font('Helvetica').fontSize(8).fillColor(ACCENT2).text(profile.company_address || '', MARGIN, 42, { width: 260 });
    doc.fillColor(ACCENT2).font('Helvetica-Bold').fontSize(20)
       .text('CLIENT STATEMENT', 300, 24, { align: 'right', width: W - 300 - MARGIN });
    doc.font('Helvetica').fontSize(8).fillColor(WHITE)
       .text(new Date().toLocaleDateString('en-GB'), 300, 52, { align: 'right', width: W - 300 - MARGIN });

    // Client info card
    let y = 115;
    doc.roundedRect(MARGIN, y, INNER_W, 56, 8).fill(LIGHT_BG);
    doc.font('Helvetica-Bold').fontSize(7).fillColor(MUTED).text('PREPARED FOR', MARGIN + 16, y + 10);
    doc.font('Helvetica-Bold').fontSize(14).fillColor(DARK).text(customerName, MARGIN + 16, y + 22);
    doc.font('Helvetica').fontSize(9).fillColor(MUTED)
       .text(`${invs.length} invoice${invs.length !== 1 ? 's' : ''}  ·  Statement Date: ${new Date().toLocaleDateString('en-GB')}`, MARGIN + 16, y + 40);
    y += 70;

    // Table header
    doc.roundedRect(MARGIN, y, INNER_W, 26, 4).fill(NAVY);
    doc.font('Helvetica-Bold').fontSize(8).fillColor(WHITE)
       .text('INVOICE #',  MARGIN + 12,            y + 8)
       .text('DATE',       MARGIN + 110,            y + 8)
       .text('SERVICE',    MARGIN + 185,            y + 8)
       .text('AMOUNT',     MARGIN + INNER_W - 115,  y + 8, { align: 'right', width: 55 })
       .text('STATUS',     MARGIN + INNER_W - 54,   y + 8, { align: 'center', width: 54 });
    y += 26;

    let totalAmount = 0;
    let paidAmount  = 0;

    invs.forEach((inv, idx) => {
      if (y > 755) { doc.addPage({ margin: 0, size: 'A4' }); y = 40; }
      const rowBg = idx % 2 === 0 ? WHITE : '#F1F5FF';
      const rowH  = 28;
      doc.rect(MARGIN, y, INNER_W, rowH).fill(rowBg);

      const isPaid  = inv.status === 'paid';
      const amount  = parseFloat(inv.total) || 0;
      totalAmount  += amount;
      if (isPaid) paidAmount += amount;

      doc.font('Helvetica-Bold').fontSize(8.5).fillColor(DARK).text(inv.invoice_id || '', MARGIN + 12, y + 9);
      doc.font('Helvetica').fontSize(8.5).fillColor(MUTED).text(inv.date || '', MARGIN + 110, y + 9);
      const svc = (inv.service || '').slice(0, 28) + ((inv.service || '').length > 28 ? '…' : '');
      doc.font('Helvetica').fontSize(8.5).fillColor(DARK).text(svc, MARGIN + 185, y + 9, { width: INNER_W - 300 });
      doc.font('Helvetica-Bold').fontSize(8.5).fillColor(DARK)
         .text(fmtAmt(inv.total), MARGIN + INNER_W - 115, y + 9, { align: 'right', width: 55 });

      if (isPaid) {
        doc.roundedRect(MARGIN + INNER_W - 50, y + 6, 42, 16, 8).fill(GREEN);
        doc.font('Helvetica-Bold').fontSize(7).fillColor(WHITE).text('PAID', MARGIN + INNER_W - 44, y + 10, { width: 36, align: 'center' });
      } else {
        doc.roundedRect(MARGIN + INNER_W - 50, y + 6, 42, 16, 8).fill(RED);
        doc.font('Helvetica-Bold').fontSize(7).fillColor(WHITE).text('UNPAID', MARGIN + INNER_W - 44, y + 10, { width: 36, align: 'center' });
      }
      y += rowH;
    });

    // Totals summary
    y += 16;
    if (y > 720) { doc.addPage({ margin: 0, size: 'A4' }); y = 40; }
    const totX = W - MARGIN - 210;
    doc.moveTo(MARGIN, y).lineTo(W - MARGIN, y).strokeColor(BORDER).lineWidth(1).stroke();
    y += 14;

    doc.font('Helvetica').fontSize(10).fillColor(MUTED)
       .text('Total Invoiced:', totX, y).text(fmtAmt(totalAmount), totX, y, { align: 'right', width: 210 });
    y += 22;
    doc.font('Helvetica').fontSize(10).fillColor(GREEN)
       .text('Amount Paid:', totX, y).text(fmtAmt(paidAmount), totX, y, { align: 'right', width: 210 });
    y += 22;

    const outstanding = totalAmount - paidAmount;
    doc.roundedRect(totX - 8, y, 218, 40, 8).fill(outstanding > 0 ? '#FEF2F2' : '#F0FDF4');
    doc.font('Helvetica-Bold').fontSize(11).fillColor(outstanding > 0 ? RED : GREEN)
       .text(outstanding > 0 ? 'OUTSTANDING:' : '✓ FULLY PAID', totX + 4, y + 13)
       .text(fmtAmt(outstanding), totX - 4, y + 13, { align: 'right', width: 210 });
    y += 56;

    // Footer
    doc.rect(0, H - 36, W, 36).fill(NAVY);
    doc.font('Helvetica').fontSize(7.5).fillColor(ACCENT2)
       .text(`Client Statement  ·  ${profile.company_name}  ·  Generated by InvoKash  ·  ${new Date().toLocaleDateString('en-GB')}`,
             MARGIN, H - 22, { align: 'center', width: INNER_W });

    doc.end();
    stream.on('finish', () => resolve({ pdfPath, invoiceCount: invs.length, total: totalAmount, paid: paidAmount, outstanding, currency: profile.currency }));
    stream.on('error', reject);
  });
}

// ─── Invoice Templates ────────────────────────────────────────────────────────
function getTemplates(userId) {
  return invoiceTemplates[userId] || [];
}

function saveTemplate(userId, template) {
  if (!invoiceTemplates[userId]) invoiceTemplates[userId] = [];
  const idx = invoiceTemplates[userId].findIndex(t => t.name === template.name);
  if (idx >= 0) {
    invoiceTemplates[userId][idx] = template;
  } else {
    if (invoiceTemplates[userId].length >= 10) return { error: 'max_templates' };
    invoiceTemplates[userId].push(template);
  }
  saveData();
  return { success: true };
}

function deleteTemplate(userId, templateName) {
  if (!invoiceTemplates[userId]) return false;
  const before = invoiceTemplates[userId].length;
  invoiceTemplates[userId] = invoiceTemplates[userId].filter(t => t.name !== templateName);
  saveData();
  return invoiceTemplates[userId].length < before;
}

// ─── Expense Tracker ──────────────────────────────────────────────────────────
async function extractExpenseData(text) {
  const res = await axios.post('https://api.anthropic.com/v1/messages',
    {
      model:      'claude-haiku-4-5-20251001',
      max_tokens: 180,
      messages:   [{
        role:    'user',
        content: `Extract expense details from this text. Return ONLY valid JSON.\n\nText: "${text.slice(0, 300)}"\n\nReturn this exact JSON:\n{\n  "description": "what the expense was for",\n  "amount": 0.00,\n  "category": "one of: Travel, Software, Office, Marketing, Subcontractors, Equipment, Other"\n}\n\nRules: amount is a positive number only, no currency symbols.`,
      }],
    },
    { headers: { 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' }, timeout: 12000 }
  );
  let raw = res.data.content[0].text.replace(/```json\n?|\n?```/g, '').trim();
  const m = raw.match(/\{[\s\S]*\}/);
  if (m) raw = m[0];
  return JSON.parse(raw);
}

function logExpense(userId, data) {
  if (!expenseHistory[userId]) expenseHistory[userId] = [];
  const expense = {
    id:          `EXP-${Date.now()}`,
    date:        new Date().toLocaleDateString('en-GB'),
    description: data.description,
    amount:      parseFloat(data.amount).toFixed(2),
    category:    EXPENSE_CATEGORIES.includes(data.category) ? data.category : 'Other',
    currency:    companyProfiles[userId]?.currency || 'AED',
  };
  expenseHistory[userId].push(expense);
  saveData();
  return expense;
}

function getExpenses(userId, period) {
  // filterInvoicesByPeriod works for any array with a date field
  return filterInvoicesByPeriod(expenseHistory[userId] || [], period);
}

function calculateProfitLoss(userId, period) {
  const invs     = filterInvoicesByPeriod(invoiceHistory[userId] || [], period);
  const exps     = getExpenses(userId, period);
  const revenue  = invs.reduce((s, i) => s + (parseFloat(i.total) || 0), 0);
  const expenses = exps.reduce((s, e) => s + (parseFloat(e.amount) || 0), 0);
  const profit   = revenue - expenses;
  const margin   = revenue > 0 ? (profit / revenue) * 100 : 0;

  const byCategory = {};
  exps.forEach(e => {
    byCategory[e.category] = (byCategory[e.category] || 0) + (parseFloat(e.amount) || 0);
  });

  return { revenue, expenses, profit, margin, byCategory, invoiceCount: invs.length, expenseCount: exps.length };
}

// ─── Lifecycle ────────────────────────────────────────────────────────────────
loadData();
setInterval(saveData, 5 * 60 * 1000);
process.on('SIGINT',  () => { saveData(); process.exit(0); });
process.on('SIGTERM', () => { saveData(); process.exit(0); });

module.exports = {
  // State (shared, mutable references)
  companyProfiles, invoiceHistory, onboardingState, commandState, pendingInvoices,
  revenueGoals, invoiceTemplates, expenseHistory,
  // Constants
  CURRENCIES, PERIOD_NAMES, LOGO_DIR, INVOICE_DIR, EXPENSE_CATEGORIES,
  // Utils
  checkRateLimit, sanitizeInput, formatAmount, getTaxConfig,
  filterInvoicesByPeriod, progressBar, asciiBar, generateInvoiceId,
  // Data
  saveData, loadData, generateCSV,
  // AI
  classifyIntent, extractInvoiceData, transcribeAudio, validateInvoiceData,
  generateBusinessInsights,
  // Business logic
  processInvoiceText, confirmInvoice, markInvoicePaid, calculateStats,
  buildDownloadZip,
  // New features
  getLastInvoiceForCustomer,
  getAgingReport,
  setRevenueGoal, getRevenueGoal,
  generateClientStatement,
  saveTemplate, getTemplates, deleteTemplate,
  extractExpenseData, logExpense, getExpenses, calculateProfitLoss,
  // PDF & Payment
  generateProfessionalInvoice, createPaymentLink,
};
