/**
 * DATA IMPORT MANAGER — Excel → Split into 3 tables mapped to CURRENT SCHEMA
 * A–W  (0..22)  -> initial_course_upload
 * X–AQ (23..42) -> google_places_data
 * AR–AS(43..44) -> review_urls
 * All inserts carry course_number from column A (col 0).
 */
export class DataImportManager {
  constructor(pipelineManager) {
    this.pipelineManager = pipelineManager;
    this.utils = pipelineManager.utils || this._fallbackUtils();

    // Fixed ranges (0-indexed)
    this.RANGES = {
      icu:  { start: 0,  end: 22 },  // A..W
      gpd:  { start: 23, end: 42 },  // X..AQ
      rurl: { start: 43, end: 44 }   // AR..AS
    };

    // Allowed columns by schema (superset; only columns inside range + header match are sent)
    this.ICU_COLS = new Set([
      // keys
      'course_number','course_name','street_address','city','county','state_or_region','zip_code',
      'phone_number','website_url','architect','year_built_founded','status_type','email_address',
      'total_par','total_holes','course_rating','slope_rating','total_length',
      // common extras that might appear early (they’ll be ignored if out of slice)
      'latitude','longitude','meta_title','meta_data_description','open_graph_title','open_graph_description',
      'facebook_url','instagram_url','twitter_url','youtube_url','tiktok_url',
      'scorecard_url','rates_url','tee_times_url','membership_url','about_url'
    ]);

    this.GPD_COLS = new Set([
      'course_number','place_id','display_name','formatted_address',
      'street_number','route','street_address','city','state','county','country',
      'latitude','longitude','primary_type','website','phone','opening_hours',
      'user_rating_count','photo_reference','google_maps_link','zip_code'
    ]);

    this.RURL_COLS = new Set([
      'course_number','golf_now_url','golf_pass_url'
    ]);

    // Header aliases → schema columns
    this.ALIAS = {
      // shared
      'course id': 'course_number',
      'courseid': 'course_number',

      // ICU-specific mapping (left side are normalized headers; see _normHeader)
      'state': 'state_or_region', // ICU expects state_or_region
      'phone': 'phone_number',
      'website': 'website_url',
      'year_built': 'year_built_founded',
      'golfnow_url': 'golf_now_url', // if it appears in icu slice we’ll keep it, then ignored by ICU_COLS
      'golfpass_url': 'golf_pass_url',

      // GPD mapping
      'display name': 'display_name',
      'user rating count': 'user_rating_count',
      'google maps link': 'google_maps_link',

      // review_urls mapping
      'golfnow url': 'golf_now_url',
      'golf pass url': 'golf_pass_url',
      'golfpass url': 'golf_pass_url'
    };
  }

  // ----------------------------- UI wiring (keep your existing shells) -----------------------------
  initializeUI() { this._createExcelUI(); }
  setupEventListeners() { this.initializeUI(); this._wireExcel(); }

  _createExcelUI() {
    let excelSection = document.querySelector('.excel-import-section') || document.querySelector('.pipeline-section');
    if (!excelSection) return;
    excelSection.innerHTML = `
      <div class="section-header">
        <h3>📊 Excel Data Import</h3>
        <p>A–W → initial_course_upload · X–AQ → google_places_data · AR–AS → review_urls</p>
      </div>
      <div class="excel-import-controls">
        <div class="file-upload-section">
          <input type="file" id="excelFileInput" accept=".xlsx,.xls" class="file-input" style="display:none;">
          <label for="excelFileInput" class="file-label btn-secondary">📁 Choose Excel File</label>
          <div style="margin-top:10px;">
            <button id="analyzeExcelBtn" class="btn-secondary" disabled>🔍 Analyze</button>
            <button id="importExcelBtn" class="btn-primary" disabled>🚀 Import</button>
            <button id="clearAllDataBtn" class="btn-warning">🗑️ Clear</button>
          </div>
        </div>
        <div id="excelAnalysisResults" class="analysis-results hidden" style="margin-top:20px;"></div>
        <div id="excelImportProgress" class="import-progress hidden" style="margin-top:20px;">
          <div class="progress-bar-container" style="width:100%;height:20px;background:#f0f0f0;border-radius:10px;overflow:hidden;">
            <div class="progress-bar" id="excelProgressBar" style="height:100%;background:linear-gradient(90deg,#4CAF50,#45a049);width:0%;transition:width .3s;"></div>
          </div>
          <div id="excelProgressText" style="margin-top:10px;text-align:center;">Ready…</div>
        </div>
      </div>
    `;
  }

  _wireExcel() {
    const fileInput  = document.getElementById('excelFileInput');
    const analyzeBtn = document.getElementById('analyzeExcelBtn');
    const importBtn  = document.getElementById('importExcelBtn');
    const clearBtn   = document.getElementById('clearAllDataBtn');

    if (fileInput)  fileInput.addEventListener('change', (e)=> this._onFileSelect(e));
    if (analyzeBtn) analyzeBtn.addEventListener('click', ()=> this.analyzeExcelData());
    if (importBtn)  importBtn.addEventListener('click',  ()=> this.importExcelData());
    if (clearBtn)   clearBtn.addEventListener('click',   ()=> this.clearAllData());
  }

  _onFileSelect(e) {
    const f = e.target.files?.[0];
    const ok = !!(f && (f.name.endsWith('.xlsx') || f.name.endsWith('.xls')));
    document.getElementById('analyzeExcelBtn')?.toggleAttribute('disabled', !ok);
    document.getElementById('importExcelBtn')?.toggleAttribute('disabled', !ok);
    this.utils.logMessage(ok ? 'info' : 'error',
      ok ? `Excel file selected: ${f.name} (${(f.size/1024/1024).toFixed(2)} MB)` :
           'Please select a valid .xlsx/.xls file');
    const res = document.getElementById('excelAnalysisResults');
    if (res) { res.innerHTML=''; res.classList.add('hidden'); }
  }

  // ----------------------------- Excel reading -----------------------------
  _readExcelAoA(file) {
    return new Promise((resolve, reject) => {
      if (typeof XLSX === 'undefined') return reject(new Error('SheetJS (XLSX) not loaded'));
      const reader = new FileReader();
      reader.onload = (e) => {
        try {
          const data = new Uint8Array(e.target.result);
          const wb   = XLSX.read(data, { type: 'array' });
          const ws   = wb.Sheets[wb.SheetNames[0]];
          const aoa  = XLSX.utils.sheet_to_json(ws, { header: 1 }); // array-of-arrays
          resolve(aoa);
        } catch (err) { reject(err); }
      };
      reader.onerror = reject;
      reader.readAsArrayBuffer(file);
    });
  }

  // ----------------------------- Normalizers & helpers -----------------------------
  _normHeader(s) {
    if (s == null) return null;
    // zero-width & NBSP → trim
    let v = String(s).replace(/[\u200B-\u200D\uFEFF]/g,'').replace(/\u00A0/g,' ').trim().toLowerCase();
    v = v.replace(/[^\w]+/g,'_').replace(/^_+|_+$/g,'');
    // apply alias
    if (this.ALIAS[v]) return this.ALIAS[v];
    return v;
  }
  _normKey(raw) {
    if (raw == null) return null;
    let v = String(raw).replace(/[\u200B-\u200D\uFEFF]/g,'').replace(/\u00A0/g,' ').trim();
    v = v.replace(/[\u2010-\u2015]/g, '-').toUpperCase(); // unify dashes + case
    return v || null;
  }
  _val(v) {
    if (v === undefined || v === null) return null;
    const s = String(v).replace(/[\u200B-\u200D\uFEFF]/g,'').replace(/\u00A0/g,' ').trim();
    return s === '' ? null : s;
  }
  _mergeNonEmpty(a, b) {
    const out = { ...a };
    for (const k of Object.keys(b)) {
      if (k === 'created_at') continue;
      const av = a[k];
      const bv = b[k];
      const aEmpty = av == null || (typeof av === 'string' && av.trim() === '');
      const bHas   = !(bv == null || (typeof bv === 'string' && bv.trim() === ''));
      if (aEmpty && bHas) out[k] = bv;
    }
    return out;
  }

  _pickRangeObject(row, headers, start, end, allowedCols, table) {
    const obj = {};
    for (let c = start; c <= end && c < row.length; c++) {
      const rawHeader = headers[c];
      if (!rawHeader) continue;
      let col = this._normHeader(rawHeader);
      // ICU special: if header says "state" we map to state_or_region (ALIAS handles this)
      if (!col || !allowedCols.has(col)) continue;
      // never override course_number from here
      if (col === 'course_number') continue;
      const v = this._val(row[c]);
      if (v !== null) obj[col] = v;
    }
    return obj;
  }

  // ----------------------------- Analyze (optional) -----------------------------
  async analyzeExcelData() {
    const file = document.getElementById('excelFileInput')?.files?.[0];
    if (!file) return this.utils.logMessage('error','Please select a file first');
    try {
      this.utils.logMessage('info','📊 Starting analysis…');
      const aoa = await this._readExcelAoA(file);
      const header = aoa[0] || [];
      const html = `
        <div>
          <p><strong>${aoa.length - 1}</strong> data rows</p>
          <p>Headers: ${header.map(h => `<code>${h}</code>`).join(', ')}</p>
        </div>`;
      const res = document.getElementById('excelAnalysisResults');
      if (res) { res.innerHTML = html; res.classList.remove('hidden'); }
      this.utils.logMessage('success','Analysis completed!');
    } catch (e) {
      this.utils.logMessage('error', `Analysis failed: ${e.message}`);
    }
  }

  // ----------------------------- Import per spec (A–W / X–AQ / AR–AS) -----------------------------
  async importExcelData() {
    try {
      const file = document.getElementById('excelFileInput')?.files?.[0];
      if (!file) return this.utils.logMessage('error','Please select an Excel file first');
      const client = this.pipelineManager?.database?.client;
      if (!client) return this.utils.logMessage('error','Database client not available');

      this.updateExcelProgress(0, 'Reading Excel file…');
      const aoa = await this._readExcelAoA(file);
      if (!Array.isArray(aoa) || aoa.length < 2) {
        return this.utils.logMessage('error','Excel file appears empty');
      }

      const headerRaw = aoa[0] || [];
      const headers   = headerRaw.map(h => this._normHeader(h));

      const icuMap  = new Map();
      const gpdMap  = new Map();
      const rurlMap = new Map();

      for (let i = 1; i < aoa.length; i++) {
        const row = aoa[i] || [];
        const rawCN = this._val(row[0]);
        const key   = this._normKey(rawCN);
        if (!key) continue;

        // Build each table object from its slice
        const icuObj  = { course_number: rawCN, ...this._pickRangeObject(row, headers, this.RANGES.icu.start,  this.RANGES.icu.end,  this.ICU_COLS,  'initial_course_upload') };
        const gpdObj  = { course_number: rawCN, ...this._pickRangeObject(row, headers, this.RANGES.gpd.start,  this.RANGES.gpd.end,  this.GPD_COLS,  'google_places_data')    };
        const rurlObj = { course_number: rawCN, ...this._pickRangeObject(row, headers, this.RANGES.rurl.start, this.RANGES.rurl.end, this.RURL_COLS, 'review_urls')           };

        // Deduplicate by normalized key (merge non-empty)
        icuMap.set(key,  icuMap.has(key)  ? this._mergeNonEmpty(icuMap.get(key),  icuObj)  : icuObj);
        gpdMap.set(key,  gpdMap.has(key)  ? this._mergeNonEmpty(gpdMap.get(key),  gpdObj)  : gpdObj);
        rurlMap.set(key, rurlMap.has(key) ? this._mergeNonEmpty(rurlMap.get(key), rurlObj) : rurlObj);
      }

      // Final arrays
      const icuRows  = Array.from(icuMap.values()).filter(r => r.course_number);
      const gpdRows  = Array.from(gpdMap.values()).filter(r => r.course_number);
      const rurlRows = Array.from(rurlMap.values()).filter(r => r.course_number);

      this.updateExcelProgress(10, `Prepared ICU=${icuRows.length} · GPD=${gpdRows.length} · URLs=${rurlRows.length}`);

      // Batch upsert helpers
      const upsertBatched = async (table, rows, onConflict='course_number') => {
        const BATCH = 500;
        for (let i = 0; i < rows.length; i += BATCH) {
          const chunk = rows.slice(i, i + BATCH);
          this.updateExcelProgress(Math.min(90, Math.round((i / rows.length) * 80) + 10), `Upserting ${table}: ${Math.min(i + BATCH, rows.length)}/${rows.length}…`);
          try {
            const { error } = await client.from(table).upsert(chunk, { onConflict });
            if (error) throw error;
          } catch (err) {
            // per-row fallback with logging
            this.utils.logMessage('warn', `Batch upsert failed for ${table} (${err.message}). Falling back to per-row…`);
            for (const r of chunk) {
              try {
                const { error } = await client.from(table).upsert(r, { onConflict });
                if (error) throw error;
              } catch (rowErr) {
                this.utils.logMessage('error', `Row failed in ${table} for course_number="${r.course_number}": ${rowErr.message}`);
              }
            }
          }
        }
      };

      // 1) initial_course_upload (unique on course_number)
      if (icuRows.length) await upsertBatched('initial_course_upload', icuRows, 'course_number');

      // 2) google_places_data (unique on course_number per schema)
      if (gpdRows.length) await upsertBatched('google_places_data', gpdRows, 'course_number');

      // 3) review_urls (NO unique on course_number): delete then insert
      if (rurlRows.length) {
        this.updateExcelProgress(90, 'Syncing review URLs…');
        const courseNumbers = [...new Set(rurlRows.map(r => r.course_number))].filter(Boolean);
        if (courseNumbers.length) {
          // delete existing for these course_numbers to avoid duplicates
          const { error: delErr } = await client.from('review_urls').delete().in('course_number', courseNumbers);
          if (delErr) this.utils.logMessage('warn', `review_urls cleanup warning: ${delErr.message}`);
        }
        // Insert in batches
        const BATCH = 500;
        for (let i = 0; i < rurlRows.length; i += BATCH) {
          const chunk = rurlRows.slice(i, i + BATCH);
          try {
            const { error } = await client.from('review_urls').insert(chunk);
            if (error) throw error;
          } catch (err) {
            this.utils.logMessage('error', `review_urls insert failed: ${err.message}`);
          }
        }
      }

      this.updateExcelProgress(100, 'Import complete!');
      this.utils.logMessage('success', `✅ Import complete: ICU=${icuRows.length} · GPD=${gpdRows.length} · URLs=${rurlRows.length}`);

      // optional UI refresh
      if (typeof this.pipelineManager?._populateTable === 'function') {
        await this.pipelineManager._populateTable();
      }

    } catch (e) {
      this.utils.logMessage('error', `Excel import failed: ${e.message}`);
    }
  }

  // ----------------------------- Clear (no-op placeholder) -----------------------------
  async clearAllData() {
    this.utils.logMessage('info', '🧹 Nothing to clear programmatically in this tool.');
  }

  // ----------------------------- Fallback logger -----------------------------
  _fallbackUtils() {
    return {
      logMessage: (type, message) => {
        const ts = new Date().toLocaleTimeString();
        console[type === 'error' ? 'error' : type === 'warn' ? 'warn' : 'log'](`[${ts}] ${message}`);
      }
    };
  }
}
