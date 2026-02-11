require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const axios = require('axios');
const PDFDocument = require('pdfkit');
const fs = require('fs');
const path = require('path');
const OpenAI = require('openai');
const archiver = require('archiver');

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

if (!TELEGRAM_TOKEN || !ANTHROPIC_API_KEY || !OPENAI_API_KEY) {
  console.error('ERROR: Missing environment variables!');
  process.exit(1);
}

const bot = new TelegramBot(TELEGRAM_TOKEN, { polling: true });
const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

const BASE_DIR = __dirname;
const INVOICE_DIR = path.join(BASE_DIR, 'invoices');
const DATA_DIR = path.join(BASE_DIR, 'data');
const PROFILES_FILE = path.join(DATA_DIR, 'profiles.json');
const HISTORY_FILE = path.join(DATA_DIR, 'history.json');
const BACKUP_DIR = path.join(BASE_DIR, 'backups');

let companyProfiles = {};
let onboardingState = {};
let invoiceHistory = {};

const userRateLimits = new Map();

function checkRateLimit(userId) {
  const now = Date.now();
  const userLimit = userRateLimits.get(userId) || { count: 0, resetTime: now + 60000 };
  if (now > userLimit.resetTime) {
    userLimit.count = 0;
    userLimit.resetTime = now + 60000;
  }
  userLimit.count++;
  userRateLimits.set(userId, userLimit);
  return userLimit.count <= 20;
}

function sanitizeInput(input) {
  if (typeof input !== 'string') return '';
  return input.replace(/[<>]/g, '').trim().slice(0, 500);
}

function ensureDirectorySecurity(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

['/tmp/logos', INVOICE_DIR, DATA_DIR, BACKUP_DIR].forEach(dir => ensureDirectorySecurity(dir));

function loadData() {
  try {
    if (fs.existsSync(PROFILES_FILE)) {
      companyProfiles = JSON.parse(fs.readFileSync(PROFILES_FILE, 'utf8'));
    }
    if (fs.existsSync(HISTORY_FILE)) {
      invoiceHistory = JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8'));
    }
  } catch (error) {
    console.error('Error loading:', error.message);
  }
}

function saveData() {
  try {
    fs.writeFileSync(PROFILES_FILE, JSON.stringify(companyProfiles, null, 2));
    fs.writeFileSync(HISTORY_FILE, JSON.stringify(invoiceHistory, null, 2));
  } catch (error) {
    console.error('Error saving:', error.message);
  }
}

loadData();
console.log('InvoKash Bot started');

function showWelcomeMessage(chatId, userId) {
  const hasProfile = companyProfiles[userId];
  if (hasProfile) {
    bot.sendMessage(chatId, 
      'Hello! Welcome to InvoKash!\n\n' +
      'Create invoices using voice or text.\n\n' +
      'Example: "Plumbing for Ahmed at Tower 1 for 500"\n\n' +
      'Commands:\n/stats /download /profile'
    );
  } else {
    bot.sendMessage(chatId, 'Hello! Welcome to InvoKash!\n\nUse /setup to begin.');
  }
}

function isGreeting(text) {
  if (!text) return false;
  const greetings = ['hi', 'hello', 'hey'];
  return greetings.some(g => text.toLowerCase().trim().startsWith(g));
}

function detectIntent(text) {
  const lower = text.toLowerCase();
  if (lower.includes('download')) {
    if (lower.includes('this month')) return { type: 'download', period: 'this_month' };
    if (lower.includes('last month')) return { type: 'download', period: 'last_month' };
    return { type: 'download', period: 'all' };
  }
  if (lower.includes('stats')) {
    if (lower.includes('this month')) return { type: 'stats', period: 'this_month' };
    if (lower.includes('last month')) return { type: 'stats', period: 'last_month' };
  }
  return null;
}

bot.on('message', async (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  const text = msg.text;

  if (!checkRateLimit(userId)) {
    bot.sendMessage(chatId, 'Too many requests. Wait a minute.');
    return;
  }

  try {
    if (text && text.startsWith('/')) {
      await handleCommand(chatId, userId, text);
      return;
    }

    if (text && isGreeting(text)) {
      showWelcomeMessage(chatId, userId);
      return;
    }

    if (msg.photo && onboardingState[userId] && onboardingState[userId].step === 'logo') {
      await handleLogoUpload(chatId, userId, msg.photo);
      return;
    }

    if (msg.voice) {
      if (!companyProfiles[userId]) {
        bot.sendMessage(chatId, 'Use /setup first.');
        return;
      }

      try {
        bot.sendMessage(chatId, 'Processing voice...');
        const file = await bot.getFile(msg.voice.file_id);
        const fileUrl = 'https://api.telegram.org/file/bot' + TELEGRAM_TOKEN + '/' + file.file_path;
        const response = await axios.get(fileUrl, { responseType: 'arraybuffer', timeout: 30000 });
        const voicePath = '/tmp/voice_' + userId + '_' + Date.now() + '.ogg';
        fs.writeFileSync(voicePath, Buffer.from(response.data));
        
        const transcription = await openai.audio.transcriptions.create({
          file: fs.createReadStream(voicePath),
          model: 'whisper-1'
        });
        
        fs.unlinkSync(voicePath);
        const transcribedText = sanitizeInput(transcription.text);
        
        if (isGreeting(transcribedText)) {
          showWelcomeMessage(chatId, userId);
          return;
        }
        
        const intent = detectIntent(transcribedText);
        if (intent) {
          if (intent.type === 'download') {
            await downloadInvoicesByPeriod(chatId, userId, intent.period);
            return;
          } else if (intent.type === 'stats') {
            await showStats(chatId, userId, intent.period);
            return;
          }
        }
        
        bot.sendMessage(chatId, 'You said: "' + transcribedText + '"\n\nCreating...');
        await processInvoiceRequest(chatId, userId, transcribedText);
      } catch (error) {
        bot.sendMessage(chatId, 'Could not process voice.');
      }
      return;
    }

    if (onboardingState[userId]) {
      await handleOnboarding(chatId, userId, text);
      return;
    }

    if (!companyProfiles[userId]) {
      bot.sendMessage(chatId, 'Use /setup to begin.');
      return;
    }

    if (text) {
      const intent = detectIntent(text);
      if (intent) {
        if (intent.type === 'download') {
          await downloadInvoicesByPeriod(chatId, userId, intent.period);
          return;
        } else if (intent.type === 'stats') {
          await showStats(chatId, userId, intent.period);
          return;
        }
      }
      await processInvoiceRequest(chatId, userId, sanitizeInput(text));
    }
  } catch (error) {
    bot.sendMessage(chatId, 'Error occurred.');
  }
});

async function handleCommand(chatId, userId, command) {
  if (command === '/start') {
    showWelcomeMessage(chatId, userId);
  } else if (command === '/setup') {
    startOnboarding(chatId, userId);
  } else if (command === '/profile') {
    showProfile(chatId, userId);
  } else if (command === '/invoices') {
    showInvoiceHistory(chatId, userId);
  } else if (command === '/download') {
    bot.sendMessage(chatId, 'Reply: "this month", "last month", or "all"');
  } else if (command === '/stats') {
    bot.sendMessage(chatId, 'Reply: "this month" or "last month"');
  }
}

function startOnboarding(chatId, userId) {
  onboardingState[userId] = { step: 'disclaimer' };
  bot.sendMessage(chatId, 
    'DISCLAIMER\n\n' +
    'InvoKash creates invoices for record-keeping only.\n' +
    'Not legally certified.\n\n' +
    'Reply "agree" to continue.'
  );
}

async function handleLogoUpload(chatId, userId, photos) {
  try {
    const photo = photos[photos.length - 1];
    const file = await bot.getFile(photo.file_id);
    const fileUrl = 'https://api.telegram.org/file/bot' + TELEGRAM_TOKEN + '/' + file.file_path;
    const response = await axios.get(fileUrl, { responseType: 'arraybuffer' });
    const logoPath = '/tmp/logos/logo_' + userId + '.jpg';
    fs.writeFileSync(logoPath, Buffer.from(response.data));
    companyProfiles[userId].logo_path = logoPath;
    delete onboardingState[userId];
    saveData();
    bot.sendMessage(chatId, 'Setup complete!');
  } catch (error) {
    delete onboardingState[userId];
  }
}

async function handleOnboarding(chatId, userId, text) {
  const state = onboardingState[userId];
  const input = text.toLowerCase().trim();
  if (!companyProfiles[userId]) companyProfiles[userId] = {};

  if (state.step === 'disclaimer' && input === 'agree') {
    onboardingState[userId].step = 'company_name';
    bot.sendMessage(chatId, 'Company name?');
  } else if (state.step === 'company_name') {
    companyProfiles[userId].company_name = sanitizeInput(text);
    onboardingState[userId].step = 'company_address';
    bot.sendMessage(chatId, 'Address?');
  } else if (state.step === 'company_address') {
    companyProfiles[userId].company_address = sanitizeInput(text);
    onboardingState[userId].step = 'trn';
    bot.sendMessage(chatId, 'TRN?');
  } else if (state.step === 'trn') {
    companyProfiles[userId].trn = sanitizeInput(text);
    onboardingState[userId].step = 'currency';
    bot.sendMessage(chatId, 'Currency? (AED/USD/EUR/INR/SAR/GBP)');
  } else if (state.step === 'currency') {
    const curr = text.toUpperCase().trim();
    if (['AED', 'USD', 'EUR', 'INR', 'SAR', 'GBP'].includes(curr)) {
      companyProfiles[userId].currency = curr;
      onboardingState[userId].step = 'bank_name';
      bot.sendMessage(chatId, 'Bank name?');
    }
  } else if (state.step === 'bank_name') {
    companyProfiles[userId].bank_name = sanitizeInput(text);
    onboardingState[userId].step = 'iban';
    bot.sendMessage(chatId, 'IBAN?');
  } else if (state.step === 'iban') {
    companyProfiles[userId].iban = sanitizeInput(text);
    onboardingState[userId].step = 'account_name';
    bot.sendMessage(chatId, 'Account name?');
  } else if (state.step === 'account_name') {
    companyProfiles[userId].account_name = sanitizeInput(text);
    onboardingState[userId].step = 'vat_enabled';
    bot.sendMessage(chatId, 'Include VAT? (yes/no)');
  } else if (state.step === 'vat_enabled') {
    if (input === 'yes') {
      companyProfiles[userId].vat_enabled = true;
      onboardingState[userId].step = 'vat_rate';
      bot.sendMessage(chatId, 'VAT %?');
    } else if (input === 'no') {
      companyProfiles[userId].vat_enabled = false;
      companyProfiles[userId].vat_rate = 0;
      onboardingState[userId].step = 'logo';
      bot.sendMessage(chatId, 'Send logo or type "skip"');
    }
  } else if (state.step === 'vat_rate') {
    const rate = parseFloat(text);
    if (!isNaN(rate) && rate >= 0 && rate <= 100) {
      companyProfiles[userId].vat_rate = rate;
      onboardingState[userId].step = 'logo';
      bot.sendMessage(chatId, 'Send logo or type "skip"');
    }
  } else if (state.step === 'logo' && input === 'skip') {
    companyProfiles[userId].logo_path = null;
    delete onboardingState[userId];
    saveData();
    bot.sendMessage(chatId, 'Setup complete!');
  }
}

function showProfile(chatId, userId) {
  const p = companyProfiles[userId];
  if (!p) {
    bot.sendMessage(chatId, 'No profile. Use /setup');
    return;
  }
  bot.sendMessage(chatId, 'Company: ' + p.company_name + '\nCurrency: ' + p.currency);
}

async function showInvoiceHistory(chatId, userId) {
  const invs = invoiceHistory[userId] || [];
  if (invs.length === 0) {
    bot.sendMessage(chatId, 'No invoices yet');
    return;
  }
  let msg = 'Invoices: ' + invs.length + '\n\n';
  invs.slice(-5).reverse().forEach(inv => {
    msg += inv.invoice_id + '\n' + inv.customer_name + '\n' + inv.total + ' ' + inv.currency + '\n\n';
  });
  bot.sendMessage(chatId, msg);
}

function filterInvoicesByPeriod(invoices, period) {
  const now = new Date();
  return invoices.filter(inv => {
    const parts = inv.date.split('/');
    const invDate = new Date(parts[2], parts[1] - 1, parts[0]);
    if (period === 'this_month') {
      return invDate.getMonth() === now.getMonth() && invDate.getFullYear() === now.getFullYear();
    } else if (period === 'last_month') {
      const last = new Date(now.getFullYear(), now.getMonth() - 1, 1);
      return invDate.getMonth() === last.getMonth() && invDate.getFullYear() === last.getFullYear();
    }
    return true;
  });
}

async function showStats(chatId, userId, period) {
  const invs = invoiceHistory[userId] || [];
  if (invs.length === 0) {
    bot.sendMessage(chatId, 'No invoices');
    return;
  }
  const filtered = filterInvoicesByPeriod(invs, period);
  if (filtered.length === 0) {
    bot.sendMessage(chatId, 'No invoices for period');
    return;
  }
  let total = 0;
  filtered.forEach(inv => total += parseFloat(inv.total));
  bot.sendMessage(chatId, 'Invoices: ' + filtered.length + '\nTotal: ' + total.toFixed(2));
}

async function downloadInvoicesByPeriod(chatId, userId, period) {
  const invs = invoiceHistory[userId] || [];
  const filtered = filterInvoicesByPeriod(invs, period);
  if (filtered.length === 0) {
    bot.sendMessage(chatId, 'No invoices');
    return;
  }
  
  try {
    bot.sendMessage(chatId, 'Creating ZIP...');
    const zipPath = '/tmp/invoices_' + userId + '_' + Date.now() + '.zip';
    const output = fs.createWriteStream(zipPath);
    const archive = archiver('zip');
    output.on('close', async () => {
      await bot.sendDocument(chatId, zipPath, { caption: filtered.length + ' invoices' });
      fs.unlinkSync(zipPath);
    });
    archive.pipe(output);
    filtered.forEach(inv => {
      if (fs.existsSync(inv.file_path)) {
        archive.file(inv.file_path, { name: path.basename(inv.file_path) });
      }
    });
    await archive.finalize();
  } catch (error) {
    bot.sendMessage(chatId, 'Error');
  }
}

async function processInvoiceRequest(chatId, userId, text) {
  try {
    bot.sendMessage(chatId, 'Creating...');
    const response = await axios.post('https://api.anthropic.com/v1/messages',
      { model: 'claude-sonnet-4-5-20250929', max_tokens: 1024,
        messages: [{ role: 'user', content: 'Extract from: "' + text + '". Return JSON: {"customer_name":"","service":"","address":"","amount":""}' }]
      },
      { headers: { 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' }}
    );

    const cleanJson = response.data.content[0].text.replace(/```json\n?|\n?```/g, '').trim();
    const data = JSON.parse(cleanJson);
    const amount = parseFloat(data.amount);
    const profile = companyProfiles[userId];
    
    let vat = 0, total = amount;
    if (profile.vat_enabled) {
      vat = amount * (profile.vat_rate / 100);
      total = amount + vat;
    }

    const invoiceId = 'INV-' + Date.now();
    const date = new Date().toLocaleDateString('en-GB');

    const fullData = { 
      customer_name: data.customer_name, service: data.service, address: data.address,
      company_name: profile.company_name, company_address: profile.company_address,
      trn: profile.trn, currency: profile.currency, bank_name: profile.bank_name,
      iban: profile.iban, account_name: profile.account_name,
      vat_enabled: profile.vat_enabled, vat_rate: profile.vat_rate,
      logo_path: profile.logo_path, invoice_id: invoiceId, date: date,
      subtotal: amount.toFixed(2), vat_amount: vat.toFixed(2), total: total.toFixed(2)
    };
    
    const pdfPath = await generateProfessionalInvoice(fullData);
    const permanentPath = path.join(INVOICE_DIR, userId + '_' + invoiceId + '.pdf');
    fs.copyFileSync(pdfPath, permanentPath);

    if (!invoiceHistory[userId]) invoiceHistory[userId] = [];
    invoiceHistory[userId].push({
      invoice_id: invoiceId, customer_name: data.customer_name, service: data.service,
      total: total.toFixed(2), vat_amount: vat.toFixed(2),
      currency: profile.currency, date: date, file_path: permanentPath
    });

    saveData();
    await bot.sendDocument(chatId, pdfPath, { caption: invoiceId + '\n' + total.toFixed(2) + ' ' + profile.currency });
    fs.unlinkSync(pdfPath);
  } catch (error) {
    bot.sendMessage(chatId, 'Error: ' + error.message);
  }
}

async function generateProfessionalInvoice(data) {
  return new Promise((resolve, reject) => {
    const pdfPath = '/tmp/invoice_' + Date.now() + '.pdf';
    const doc = new PDFDocument({ margin: 50, size: 'A4' });
    const stream = fs.createWriteStream(pdfPath);
    doc.pipe(stream);

    if (data.logo_path && fs.existsSync(data.logo_path)) {
      try { doc.image(data.logo_path, 50, 45, { width: 80, height: 60 }); }
      catch (err) { doc.fontSize(10).text('[LOGO]', 50, 50); }
    } else {
      doc.fontSize(10).text('[LOGO]', 50, 50);
    }

    doc.fontSize(18).text('INVOICE', 400, 50, { align: 'right' });
    doc.fontSize(8).text('(Record-keeping)', 400, 72, { align: 'right' });

    let y = 120;
    doc.fontSize(10).font('Helvetica-Bold').text('FROM:', 50, y).text('DETAILS:', 350, y);
    y += 20;
    doc.font('Helvetica').text(data.company_name, 50, y).text('Invoice: ' + data.invoice_id, 350, y);
    y += 15;
    doc.text(data.company_address, 50, y, {width: 250}).text('Date: ' + data.date, 350, y);
    y += 30;
    doc.text('TRN: ' + data.trn, 50, y);
    y += 40;
    doc.moveTo(50, y).lineTo(550, y).stroke();
    y += 20;
    doc.font('Helvetica-Bold').text('BILL TO:', 50, y);
    y += 20;
    doc.font('Helvetica').text(data.customer_name, 50, y);
    if (data.address !== 'N/A') { y += 15; doc.text(data.address, 50, y); }
    y += 30;
    doc.moveTo(50, y).lineTo(550, y).stroke();
    y += 20;
    doc.font('Helvetica-Bold').text('DESCRIPTION', 50, y).text('AMOUNT', 400, y);
    y += 20;
    doc.moveTo(50, y).lineTo(550, y).stroke();
    y += 20;
    doc.font('Helvetica').text(data.service, 50, y, {width: 300}).text(data.subtotal, 400, y);
    y += 40;
    doc.moveTo(50, y).lineTo(550, y).stroke();
    y += 20;
    doc.text('Subtotal:', 350, y).text(data.subtotal, 480, y);
    y += 20;
    if (data.vat_enabled && parseFloat(data.vat_amount) > 0) {
      doc.text('VAT (' + data.vat_rate + '%):', 350, y).text(data.vat_amount, 480, y);
      y += 15;
    }
    doc.moveTo(350, y).lineTo(550, y).stroke();
    y += 15;
    doc.font('Helvetica-Bold').fontSize(12).text('TOTAL:', 350, y).text(data.total, 480, y);
    y += 40;
    doc.fontSize(10).text('PAYMENT:', 50, y);
    y += 20;
    doc.font('Helvetica').text('Bank: ' + data.bank_name, 50, y);
    y += 15;
    doc.text('IBAN: ' + data.iban, 50, y);
    y += 15;
    doc.text('Account: ' + data.account_name, 50, y);
    doc.end();

    stream.on('finish', () => resolve(pdfPath));
    stream.on('error', reject);
  });
}

bot.on('polling_error', (error) => console.error('Error:', error.message));
process.on('SIGINT', () => { saveData(); process.exit(0); });
setInterval(saveData, 5 * 60 * 1000);
console.log('Ready!');
