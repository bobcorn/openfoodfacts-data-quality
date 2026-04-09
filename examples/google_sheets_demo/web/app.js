const DRIVE_FILE_SCOPE = "https://www.googleapis.com/auth/drive.file";
const GOOGLE_SHEETS_API_ROOT = "https://sheets.googleapis.com/v4/spreadsheets";
const WHITE_BACKGROUND = "#FFFFFF";
const SELECTED_SPREADSHEET_STORAGE_KEY =
  "off-google-sheets-demo:selected-spreadsheet";
const GOOGLE_SESSION_STORAGE_KEY =
  "off-google-sheets-demo:google-session";
const GOOGLE_PICKER_DIALOG_SELECTOR = ".picker-dialog";

let pickerLibraryPromise = null;

const state = {
  config: null,
  tokenClient: null,
  accessToken: "",
  tokenExpiresAt: 0,
  busy: false,
  pickerLoaded: false,
  selectedSpreadsheet: null,
  spreadsheetMetadataCache: new Map(),
  uploadCandidatesPrepared: false,
  tooltipElement: null,
  pendingTokenRequestReject: null,
};

const elements = {
  advancedActions: document.querySelector("#advanced-actions"),
  loginButton: document.querySelector("#login-button"),
  disconnectButton: document.querySelector("#disconnect-button"),
  clearSpreadsheetButton: document.querySelector("#clear-spreadsheet-button"),
  chooseSpreadsheetButton: document.querySelector("#choose-spreadsheet-button"),
  openSpreadsheetButton: document.querySelector("#open-spreadsheet-button"),
  connectionState: document.querySelector("#connection-state"),
  selectedSheetName: document.querySelector("#selected-sheet-name"),
  selectedSheetMeta: document.querySelector("#selected-sheet-meta"),
  dataSheetNameInput: document.querySelector("#data-sheet-name"),
  readySheetNameInput: document.querySelector("#ready-sheet-name"),
  sheetStatus: document.querySelector("#sheet-status"),
  csvFileInput: document.querySelector("#csv-file-input"),
  uploadCsvButton: document.querySelector("#upload-csv-button"),
  dataStatus: document.querySelector("#data-status"),
  validateButton: document.querySelector("#validate-button"),
  clearButton: document.querySelector("#clear-button"),
  prepareButton: document.querySelector("#prepare-button"),
  uploadOffButton: document.querySelector("#upload-off-button"),
  actionStatus: document.querySelector("#action-status"),
  summaryValidated: document.querySelector("#summary-validated"),
  summaryIssues: document.querySelector("#summary-issues"),
  summaryErrors: document.querySelector("#summary-errors"),
  loadPanel: document.querySelector("#load-panel"),
  actionsPanel: document.querySelector("#actions-panel"),
  summaryPanel: document.querySelector("#summary-panel"),
  tooltip: document.querySelector("#tooltip"),
};

window.addEventListener("DOMContentLoaded", () => {
  void init();
});

async function init() {
  bindEvents();
  initializeTooltips();
  await loadPublicConfig();
  restoreSelectedSpreadsheet();
  restoreStoredGoogleSession();
  renderSelectedSpreadsheet();
  resetSummary();
  await configureGoogleClients();
  updateActionAvailability();
}

function bindEvents() {
  elements.dataSheetNameInput.addEventListener("input", () => {
    if (state.uploadCandidatesPrepared) {
      resetUploadCandidatesPrepared();
    }
    renderSelectedSpreadsheet();
    updateActionAvailability();
  });
  elements.readySheetNameInput.addEventListener("input", () => {
    if (state.uploadCandidatesPrepared) {
      resetUploadCandidatesPrepared();
    }
    renderSelectedSpreadsheet();
    updateActionAvailability();
  });
  elements.loginButton.addEventListener("click", () => {
    void runWithUiLock(handleLogin, {
      statusElement: elements.sheetStatus,
      pendingMessage: "Opening Google sign in...",
    });
  });
  elements.disconnectButton.addEventListener("click", () => {
    void runWithUiLock(handleDisconnect, {
      statusElement: elements.sheetStatus,
      pendingMessage: "Disconnecting Google session...",
    });
  });
  elements.clearSpreadsheetButton.addEventListener("click", () => {
    void runWithUiLock(handleClearSpreadsheetSelection, {
      statusElement: elements.sheetStatus,
      pendingMessage: "Clearing spreadsheet selection...",
    });
  });
  elements.chooseSpreadsheetButton.addEventListener("click", () => {
    void runWithUiLock(handleChooseSpreadsheet, {
      statusElement: elements.sheetStatus,
      pendingMessage: "Opening Google Drive Picker...",
    });
  });
  elements.openSpreadsheetButton.addEventListener("click", () => {
    const url = currentSpreadsheetUrl();
    window.open(url, "_blank", "noopener,noreferrer");
  });
  elements.uploadCsvButton.addEventListener("click", () => {
    void runWithUiLock(handleUploadCsv, {
      statusElement: elements.dataStatus,
      pendingMessage: "Preparing CSV upload...",
    });
  });
  elements.validateButton.addEventListener("click", () => {
    void runWithUiLock(handleValidate, {
      statusElement: elements.actionStatus,
      pendingMessage: "Preparing validation...",
    });
  });
  elements.clearButton.addEventListener("click", () => {
    void runWithUiLock(handleClearValidationOutput, {
      statusElement: elements.actionStatus,
      pendingMessage: "Preparing clear action...",
    });
  });
  elements.prepareButton.addEventListener("click", () => {
    void runWithUiLock(handlePrepareUploadCandidates, {
      statusElement: elements.actionStatus,
      pendingMessage: "Preparing candidate rows...",
    });
  });
  elements.uploadOffButton.addEventListener("click", () => {
    void runWithUiLock(handleUploadToOpenFoodFacts, {
      statusElement: elements.actionStatus,
      pendingMessage: "Checking upload readiness...",
    });
  });
}

async function loadPublicConfig() {
  const config = await fetchJson("/api/config");
  state.config = config;
  elements.dataSheetNameInput.value = config.dataSheetName ?? "Data";
  elements.readySheetNameInput.value = config.readySheetName ?? "Ready for OFF upload";
}

function restoreSelectedSpreadsheet() {
  const raw = readLocalStorage(SELECTED_SPREADSHEET_STORAGE_KEY);
  if (!raw) {
    return;
  }

  try {
    const parsed = JSON.parse(raw);
    const id = toNonEmptyString(parsed?.id);
    const name = toNonEmptyString(parsed?.name);
    const url = toNonEmptyString(parsed?.url) || buildSpreadsheetUrl(id);
    if (!id || !name) {
      return;
    }
    state.selectedSpreadsheet = { id, name, url };
  } catch {
    writeLocalStorage(SELECTED_SPREADSHEET_STORAGE_KEY, "");
  }
}

async function configureGoogleClients() {
  if (!state.config?.googleClientId) {
    setSheetStatus("This app build is not configured for Google login yet.", "warning");
    renderAuthState();
    return;
  }

  await waitForGoogleIdentityServices();
  state.tokenClient = google.accounts.oauth2.initTokenClient({
    client_id: state.config.googleClientId,
    scope: DRIVE_FILE_SCOPE,
    callback: () => {},
    error_callback: (error) => {
      if (typeof state.pendingTokenRequestReject !== "function") {
        return;
      }
      const rejectPendingRequest = state.pendingTokenRequestReject;
      state.pendingTokenRequestReject = null;
      rejectPendingRequest(
        new Error(describeGooglePopupError(error?.type)),
      );
    },
  });

  if (!hasPickerConfiguration()) {
    setSheetStatus(
      "This app build is missing Google Picker configuration.",
      "warning",
    );
    renderAuthState();
    return;
  }

  await loadGooglePickerLibrary();
  const restoredSession = await restoreGoogleSession({ trySilentRefresh: true });
  if (!restoredSession) {
    clearSelectedSpreadsheet();
  }
  clearStatus(elements.sheetStatus);
  renderAuthState();
}

function hasPickerConfiguration() {
  return Boolean(
    state.config?.googleApiKey && state.config?.googleCloudProjectNumber,
  );
}

async function waitForGoogleIdentityServices() {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (window.google?.accounts?.oauth2) {
      return;
    }
    await sleep(100);
  }
  throw new Error("Google Identity Services did not finish loading.");
}

async function loadGooglePickerLibrary() {
  if (state.pickerLoaded) {
    return;
  }
  if (!pickerLibraryPromise) {
    pickerLibraryPromise = (async () => {
      for (let attempt = 0; attempt < 100; attempt += 1) {
        if (window.gapi?.load) {
          break;
        }
        await sleep(100);
      }
      if (!window.gapi?.load) {
        throw new Error("Google Picker did not finish loading.");
      }
      await new Promise((resolve, reject) => {
        window.gapi.load("picker", {
          callback: resolve,
          onerror: () => reject(new Error("Google Picker could not load.")),
          timeout: 5000,
          ontimeout: () =>
            reject(new Error("Google Picker timed out while loading.")),
        });
      });
      state.pickerLoaded = true;
    })();
  }
  await pickerLibraryPromise;
}

async function runWithUiLock(action, options = {}) {
  if (state.busy) {
    return;
  }
  state.busy = true;
  if (options.statusElement && options.pendingMessage) {
    setStatus(options.statusElement, options.pendingMessage, "neutral", {
      loading: true,
    });
  }
  updateActionAvailability();
  try {
    await action();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (options.statusElement) {
      setStatus(options.statusElement, message, "error");
    }
  } finally {
    state.busy = false;
    updateActionAvailability();
  }
}

function updateActionAvailability() {
  const hasAuthConfiguration = Boolean(state.tokenClient);
  const hasPickerConfigurationValues = hasPickerConfiguration() && state.pickerLoaded;
  const hasAuth = hasUsableGoogleSession();
  const hasSpreadsheet = Boolean(state.selectedSpreadsheet?.id);

  renderAuthState();
  elements.advancedActions.hidden = !hasAuth && !hasSpreadsheet;
  elements.loginButton.hidden = !hasAuth;
  elements.loginButton.disabled = state.busy || !hasAuthConfiguration;
  elements.disconnectButton.hidden = !hasAuth;
  elements.disconnectButton.disabled = state.busy || !hasAuth;
  elements.clearSpreadsheetButton.hidden = !hasSpreadsheet;
  elements.clearSpreadsheetButton.disabled = state.busy || !hasSpreadsheet;
  elements.chooseSpreadsheetButton.disabled =
    state.busy || !hasAuthConfiguration || !hasPickerConfigurationValues;
  elements.openSpreadsheetButton.disabled = !hasSpreadsheet;
  const flowReady = hasSpreadsheet && hasAuth;
  updateFlowStepVisibility(flowReady);

  const actionsEnabled = !state.busy && hasSpreadsheet && hasAuth;
  elements.uploadCsvButton.disabled = !actionsEnabled;
  elements.validateButton.disabled = !actionsEnabled;
  elements.clearButton.disabled = !actionsEnabled;
  elements.prepareButton.disabled = !actionsEnabled;
  const canAttemptOffUpload = actionsEnabled && state.uploadCandidatesPrepared;
  elements.uploadOffButton.disabled = state.busy || !hasSpreadsheet || !hasAuth;
  elements.uploadOffButton.setAttribute(
    "aria-disabled",
    canAttemptOffUpload ? "false" : "true",
  );
  elements.uploadOffButton.dataset.tooltip = canAttemptOffUpload
    ? "Upload the prepared rows to Open Food Facts."
    : "Run Prepare upload candidates first.";
}

function updateFlowStepVisibility(flowReady) {
  toggleFlowStep(elements.loadPanel, flowReady);
  toggleFlowStep(elements.actionsPanel, flowReady);
  toggleFlowStep(elements.summaryPanel, flowReady);
}

function toggleFlowStep(panel, visible) {
  panel.classList.toggle("panel-flow-step--hidden", !visible);
  panel.setAttribute("aria-hidden", visible ? "false" : "true");
  panel.inert = !visible;
}

async function handleLogin() {
  await ensureGoogleAccessToken({
    interactive: true,
    promptOverride: "select_account consent",
    forcePrompt: true,
  });
  setSheetStatus("Google account is ready.", "success");
}

async function handleDisconnect() {
  if (state.accessToken && window.google?.accounts?.oauth2?.revoke) {
    await new Promise((resolve) => {
      window.google.accounts.oauth2.revoke(state.accessToken, () => {
        resolve();
      });
    });
  }
  clearGoogleSession();
  clearSelectedSpreadsheet();
  updateActionAvailability();
  clearStatus(elements.sheetStatus);
  setSheetStatus(
    "Google session cleared for this browser tab. Spreadsheet selection removed.",
    "success",
  );
}

async function handleClearSpreadsheetSelection() {
  if (!state.selectedSpreadsheet) {
    setSheetStatus("No spreadsheet is currently selected.", "neutral");
    return;
  }
  const previousName = state.selectedSpreadsheet.name;
  clearSelectedSpreadsheet();
  setSheetStatus(`Cleared ${previousName}.`, "success");
}

async function ensureGoogleAccessToken({
  interactive,
  promptOverride = "",
  forcePrompt = false,
}) {
  if (!forcePrompt && hasUsableGoogleSession()) {
    return state.accessToken;
  }
  if (!state.tokenClient) {
    throw new Error("Google login is not configured for this app build.");
  }

  const prompt = promptOverride || (interactive ? "consent" : "");
  const response = await requestGoogleAccessToken(prompt);
  if (!response.access_token) {
    throw new Error("Google did not return an access token.");
  }
  const expiresIn = Number(response.expires_in ?? 0);
  storeGoogleSession({
    accessToken: response.access_token,
    tokenExpiresAt:
      expiresIn > 0 ? Date.now() + expiresIn * 1000 : Date.now() + 3000,
  });
  updateActionAvailability();
  return state.accessToken;
}

function requestGoogleAccessToken(prompt) {
  return new Promise((resolve, reject) => {
    state.pendingTokenRequestReject = reject;
    state.tokenClient.callback = (response) => {
      state.pendingTokenRequestReject = null;
      if (response.error) {
        reject(new Error(response.error));
        return;
      }
      resolve(response);
    };
    state.tokenClient.requestAccessToken({ prompt });
  });
}

function describeGooglePopupError(type) {
  if (type === "popup_closed") {
    return "Google sign in was closed before the flow finished.";
  }
  if (type === "popup_failed_to_open") {
    return "Google sign in could not open a popup window.";
  }
  return "Google sign in did not complete.";
}

async function handleChooseSpreadsheet() {
  await ensureGoogleAccessToken({ interactive: true });
  setSheetStatus("Choose a Google Sheet.", "neutral", {
    loading: true,
  });
  const selection = await openSpreadsheetPicker();
  if (!selection) {
    setSheetStatus("Spreadsheet selection cancelled.", "neutral");
    return;
  }
  setSelectedSpreadsheet(selection);
  setSheetStatus(`Connected ${selection.name}.`, "success");
}

async function openSpreadsheetPicker() {
  if (!state.pickerLoaded) {
    throw new Error("Google Picker is not configured for this app build.");
  }

  const pickerNamespace = window.google?.picker;
  if (!pickerNamespace) {
    throw new Error("Google Picker is not available in the browser.");
  }

  return new Promise((resolve, reject) => {
    const scrollPosition = { x: window.scrollX, y: window.scrollY };
    let settled = false;
    let sawPickerDialog = false;
    let closeWatcherId = 0;
    let openTimeoutId = 0;
    const stopWatching = () => {
      if (closeWatcherId) {
        window.clearInterval(closeWatcherId);
        closeWatcherId = 0;
      }
      if (openTimeoutId) {
        window.clearTimeout(openTimeoutId);
        openTimeoutId = 0;
      }
    };
    const settleResolve = (value) => {
      if (settled) {
        return;
      }
      settled = true;
      stopWatching();
      resolve(value);
    };
    const settleReject = (error) => {
      if (settled) {
        return;
      }
      settled = true;
      stopWatching();
      reject(error);
    };
    const view = new pickerNamespace.DocsView(pickerNamespace.ViewId.SPREADSHEETS)
      .setSelectFolderEnabled(false)
      .setMimeTypes("application/vnd.google-apps.spreadsheet");

    const picker = new pickerNamespace.PickerBuilder()
      .setAppId(state.config.googleCloudProjectNumber)
      .setDeveloperKey(state.config.googleApiKey)
      .setOAuthToken(state.accessToken)
      .setOrigin(window.location.origin)
      .addView(view)
      .setTitle("Choose a Google Sheet")
      .setCallback((data) => {
        if (settled) {
          return;
        }
        const action = data?.[pickerNamespace.Response.ACTION];
        if (action === pickerNamespace.Action.CANCEL) {
          settleResolve(null);
          return;
        }
        if (action !== pickerNamespace.Action.PICKED) {
          return;
        }

        const documents = Array.isArray(
          data?.[pickerNamespace.Response.DOCUMENTS],
        )
          ? data[pickerNamespace.Response.DOCUMENTS]
          : [];
        const document = documents[0];
        const id = toNonEmptyString(document?.[pickerNamespace.Document.ID]);
        const name =
          toNonEmptyString(document?.[pickerNamespace.Document.NAME]) ||
          "Selected spreadsheet";
        if (!id) {
          settleReject(new Error("Google Picker did not return a spreadsheet ID."));
          return;
        }

        const url =
          toNonEmptyString(document?.[pickerNamespace.Document.URL]) ||
          buildSpreadsheetUrl(id);
        settleResolve({ id, name, url });
      })
      .build();

    picker.setVisible(true);
    scheduleScrollRestore(scrollPosition);
    closeWatcherId = window.setInterval(() => {
      const dialog = document.querySelector(GOOGLE_PICKER_DIALOG_SELECTOR);
      if (dialog) {
        sawPickerDialog = true;
        return;
      }
      if (sawPickerDialog) {
        settleResolve(null);
      }
    }, 150);
    openTimeoutId = window.setTimeout(() => {
      if (!sawPickerDialog) {
        settleReject(new Error("Google Picker did not open correctly."));
      }
    }, 8000);
  });
}

function scheduleScrollRestore(position) {
  const retryDelaysMs = [0, 60, 150, 300, 600];
  for (const delay of retryDelaysMs) {
    window.setTimeout(() => {
      window.scrollTo(position.x, position.y);
    }, delay);
  }
}

function setSelectedSpreadsheet(selection) {
  state.selectedSpreadsheet = selection;
  primeSpreadsheetMetadataCache(selection.id, null);
  resetUploadCandidatesPrepared();
  clearStatus(elements.dataStatus);
  clearStatus(elements.actionStatus);
  resetSummary();
  writeLocalStorage(
    SELECTED_SPREADSHEET_STORAGE_KEY,
    JSON.stringify(selection),
  );
  renderSelectedSpreadsheet();
  updateActionAvailability();
}

function clearSelectedSpreadsheet() {
  state.selectedSpreadsheet = null;
  state.spreadsheetMetadataCache.clear();
  resetUploadCandidatesPrepared();
  writeLocalStorage(SELECTED_SPREADSHEET_STORAGE_KEY, "");
  clearStatus(elements.dataStatus);
  clearStatus(elements.actionStatus);
  resetSummary();
  renderSelectedSpreadsheet();
  updateActionAvailability();
}

function renderSelectedSpreadsheet() {
  const selection = state.selectedSpreadsheet;
  if (!selection) {
    const hasAuth = hasUsableGoogleSession();
    elements.selectedSheetName.textContent = "No spreadsheet selected yet.";
    elements.selectedSheetMeta.textContent = hasAuth
      ? "Choose a Sheet from Google Drive to enable the actions."
      : "Choose a Sheet to sign in and enable the actions.";
    return;
  }

  elements.selectedSheetName.textContent = selection.name;
  elements.selectedSheetMeta.textContent =
    `Data tab: ${currentDataSheetName()} | Output tab: ${currentReadySheetName()}`;
}

function renderAuthState() {
  const hasAuth = hasUsableGoogleSession();
  const hasSpreadsheet = Boolean(state.selectedSpreadsheet);
  elements.connectionState.textContent = hasAuth
    ? "Google connected."
    : "Google not connected. Choose spreadsheet asks for access when needed.";
  elements.loginButton.textContent = hasAuth ? "Change Google account" : "Connect Google";
  if (!hasSpreadsheet) {
    renderSelectedSpreadsheet();
  }
}

async function handleUploadCsv() {
  const spreadsheetId = currentSpreadsheetId();
  const file = elements.csvFileInput.files?.[0];
  if (!file) {
    throw new Error("Choose a CSV file before uploading.");
  }

  await ensureGoogleAccessToken({ interactive: false });
  setDataStatus("Reading the CSV file...", "neutral", {
    loading: true,
  });
  const csvText = await file.text();
  const parsed = await postJson("/api/parse-csv", { csvText });
  const dataSheetName = currentDataSheetName();
  setDataStatus(`Uploading ${parsed.table.rows.length} rows into ${dataSheetName}...`, "neutral", {
    loading: true,
  });
  await replaceSheetTable({
    spreadsheetId,
    sheetName: dataSheetName,
    table: parsed.table,
    rowBackgrounds: {},
  });
  resetUploadCandidatesPrepared();
  resetSummary();
  setDataStatus(
    `Loaded ${parsed.table.rows.length} rows into ${dataSheetName}.`,
    "success",
  );
}

async function handleValidate() {
  const spreadsheetId = currentSpreadsheetId();
  await ensureGoogleAccessToken({ interactive: false });
  setActionStatus("Reading the Data tab...", "neutral", {
    loading: true,
  });
  const dataSheetName = currentDataSheetName();
  const { table, sheetProperties } = await readSheetTable(
    spreadsheetId,
    dataSheetName,
  );
  setActionStatus("Running Python checks on the current rows...", "neutral", {
    loading: true,
  });
  const response = await postJson("/api/validate", { table });
  setActionStatus("Writing dq_* columns and row colors to the sheet...", "neutral", {
    loading: true,
  });
  await replaceSheetTable({
    spreadsheetId,
    sheetName: dataSheetName,
    table: response.table,
    rowBackgrounds: response.rowBackgrounds ?? {},
    sheetProperties,
  });
  resetUploadCandidatesPrepared();
  updateSummary({
    validatedRows: response.validatedRows ?? 0,
    issueRows: response.issueRows ?? 0,
    errorRows: response.errorRows ?? 0,
  });
  setActionStatus(
    `Validation complete. Checked ${response.validatedRows ?? 0} rows, with ${response.issueRows ?? 0} rows that have findings.`,
    "success",
  );
}

async function handleClearValidationOutput() {
  const spreadsheetId = currentSpreadsheetId();
  await ensureGoogleAccessToken({ interactive: false });
  setActionStatus("Reading the Data tab...", "neutral", {
    loading: true,
  });
  const dataSheetName = currentDataSheetName();
  const { table, sheetProperties } = await readSheetTable(
    spreadsheetId,
    dataSheetName,
  );
  const clearedTable = stripValidationColumnsInBrowser(table);
  setActionStatus("Removing dq_* columns and clearing row formatting...", "neutral", {
    loading: true,
  });
  await replaceSheetTable({
    spreadsheetId,
    sheetName: dataSheetName,
    table: clearedTable,
    rowBackgrounds: {},
    sheetProperties,
  });
  resetUploadCandidatesPrepared();
  resetSummary();
  setActionStatus(`Removed derived validation columns from ${dataSheetName}.`, "success");
}

async function handlePrepareUploadCandidates() {
  const spreadsheetId = currentSpreadsheetId();
  await ensureGoogleAccessToken({ interactive: false });
  setActionStatus("Reading rows from the Data tab...", "neutral", {
    loading: true,
  });
  const dataSheetName = currentDataSheetName();
  const readySheetName = currentReadySheetName();
  const { table } = await readSheetTable(spreadsheetId, dataSheetName);
  const candidateTable = prepareUploadCandidatesInBrowser(table);
  setActionStatus(`Writing only clean rows into ${readySheetName}...`, "neutral", {
    loading: true,
  });
  await replaceSheetTable({
    spreadsheetId,
    sheetName: readySheetName,
    table: candidateTable,
    rowBackgrounds: {},
  });
  state.uploadCandidatesPrepared = true;
  updateActionAvailability();
  setActionStatus(
    `Prepared ${candidateTable.rows.length} upload candidates in ${readySheetName}.`,
    "success",
  );
}

async function handleUploadToOpenFoodFacts() {
  if (!state.uploadCandidatesPrepared) {
    setActionStatus("Run Prepare upload candidates before this step.", "warning");
    return;
  }
  setActionStatus(
    "Upload to Open Food Facts is not available yet.",
    "warning",
  );
}

function resetUploadCandidatesPrepared() {
  state.uploadCandidatesPrepared = false;
  updateActionAvailability();
}

function resetSummary() {
  updateSummary({
    validatedRows: 0,
    issueRows: 0,
    errorRows: 0,
  });
}

function updateSummary({ validatedRows, issueRows, errorRows }) {
  elements.summaryValidated.textContent = String(validatedRows);
  elements.summaryIssues.textContent = String(issueRows);
  elements.summaryErrors.textContent = String(errorRows);
}

function setSheetStatus(message, kind, options) {
  setStatus(elements.sheetStatus, message, kind, options);
}

function setDataStatus(message, kind, options) {
  setStatus(elements.dataStatus, message, kind, options);
}

function setActionStatus(message, kind, options) {
  setStatus(elements.actionStatus, message, kind, options);
}

function setStatus(element, message, kind, options = {}) {
  element.hidden = false;
  element.textContent = message;
  element.className = `status status-${kind}`;
  if (options.loading) {
    element.classList.add("is-loading");
  }
}

function clearStatus(element) {
  element.hidden = true;
  element.textContent = "";
  element.className = "status status-neutral";
}

function hasUsableGoogleSession() {
  return Boolean(state.accessToken) && Date.now() < state.tokenExpiresAt - 30_000;
}

async function restoreGoogleSession(options = {}) {
  restoreStoredGoogleSession();
  if (hasUsableGoogleSession()) {
    return true;
  }
  clearGoogleSession();
  if (!options.trySilentRefresh || !state.tokenClient) {
    return false;
  }
  try {
    const response = await requestGoogleAccessToken("");
    if (!response.access_token) {
      return false;
    }
    const expiresIn = Number(response.expires_in ?? 0);
    storeGoogleSession({
      accessToken: response.access_token,
      tokenExpiresAt:
        expiresIn > 0 ? Date.now() + expiresIn * 1000 : Date.now() + 3000,
    });
    return true;
  } catch {
    clearGoogleSession();
    return false;
  }
}

function storeGoogleSession({ accessToken, tokenExpiresAt }) {
  state.accessToken = accessToken;
  state.tokenExpiresAt = tokenExpiresAt;
  writeSessionStorage(
    GOOGLE_SESSION_STORAGE_KEY,
    JSON.stringify({
      accessToken,
      tokenExpiresAt,
    }),
  );
}

function restoreStoredGoogleSession() {
  const raw = readSessionStorage(GOOGLE_SESSION_STORAGE_KEY);
  if (!raw) {
    return;
  }

  try {
    const parsed = JSON.parse(raw);
    const accessToken = toNonEmptyString(parsed?.accessToken);
    const tokenExpiresAt = Number(parsed?.tokenExpiresAt ?? 0);
    if (!accessToken || !Number.isFinite(tokenExpiresAt)) {
      clearGoogleSession();
      return;
    }
    state.accessToken = accessToken;
    state.tokenExpiresAt = tokenExpiresAt;
  } catch {
    clearGoogleSession();
  }
}

function clearGoogleSession() {
  state.accessToken = "";
  state.tokenExpiresAt = 0;
  writeSessionStorage(GOOGLE_SESSION_STORAGE_KEY, "");
}

function currentSpreadsheetId() {
  const id = state.selectedSpreadsheet?.id;
  if (!id) {
    throw new Error("Choose a Google Sheet before running this action.");
  }
  return id;
}

function currentSpreadsheetUrl() {
  const id = currentSpreadsheetId();
  return state.selectedSpreadsheet?.url || buildSpreadsheetUrl(id);
}

function currentDataSheetName() {
  const value = elements.dataSheetNameInput.value.trim();
  return value || "Data";
}

function currentReadySheetName() {
  const value = elements.readySheetNameInput.value.trim();
  return value || "Ready for OFF upload";
}

function stripValidationColumnsInBrowser(table) {
  const keepIndices = table.headers.reduce((indices, header, index) => {
    if (!header.startsWith("dq_")) {
      indices.push(index);
    }
    return indices;
  }, []);
  return {
    headers: keepIndices.map((index) => table.headers[index]),
    rows: table.rows.map((row) => keepIndices.map((index) => toStringCell(row[index]))),
  };
}

function prepareUploadCandidatesInBrowser(table) {
  const statusIndex = table.headers.indexOf("dq_status");
  if (statusIndex < 0) {
    throw new Error("Validate the Data sheet before preparing upload candidates.");
  }

  const keepIndices = table.headers.reduce((indices, header, index) => {
    if (!header.startsWith("dq_")) {
      indices.push(index);
    }
    return indices;
  }, []);
  return {
    headers: keepIndices.map((index) => table.headers[index]),
    rows: table.rows
      .filter((row) => toStringCell(row[statusIndex]) === "ok")
      .map((row) => keepIndices.map((index) => toStringCell(row[index]))),
  };
}

function buildSpreadsheetUrl(spreadsheetId) {
  return `https://docs.google.com/spreadsheets/d/${spreadsheetId}/edit`;
}

async function readSheetTable(spreadsheetId, sheetName) {
  const sheetProperties = await ensureSheet(spreadsheetId, sheetName);
  const range = encodeURIComponent(`'${sheetName}'`);
  const response = await googleApiJson(
    `${GOOGLE_SHEETS_API_ROOT}/${spreadsheetId}/values/${range}`,
    {
      method: "GET",
    },
  );
  const values = Array.isArray(response.values) ? response.values : [];
  if (values.length === 0) {
    return { table: { headers: [], rows: [] }, sheetProperties };
  }

  const headerRow = Array.isArray(values[0]) ? values[0].map(toStringCell) : [];
  const width = headerRow.length;
  const rows = values.slice(1).map((row) => normaliseRow(row, width));
  return {
    table: {
      headers: headerRow,
      rows,
    },
    sheetProperties,
  };
}

async function replaceSheetTable({
  spreadsheetId,
  sheetName,
  table,
  rowBackgrounds,
  sheetProperties = null,
}) {
  let properties = sheetProperties ?? (await ensureSheet(spreadsheetId, sheetName));
  properties = await ensureSheetCapacity({
    spreadsheetId,
    sheetName,
    properties,
    rowsNeeded: Math.max(table.rows.length + 1, 1),
    columnsNeeded: Math.max(table.headers.length, 1),
  });
  await clearSheetValues(spreadsheetId, sheetName);
  if (table.headers.length > 0) {
    await updateSheetValues(spreadsheetId, sheetName, [table.headers, ...table.rows]);
  }
  await applyRowBackgrounds({
    spreadsheetId,
    properties,
    rowBackgrounds,
    columnCount: Math.max(properties.columnCount, table.headers.length, 1),
  });
}

async function ensureSheet(spreadsheetId, sheetName) {
  const metadata = await fetchSpreadsheetMetadataCached(spreadsheetId);
  const existing = findSheetProperties(metadata, sheetName);
  if (existing) {
    return existing;
  }

  const response = await googleApiJson(`${GOOGLE_SHEETS_API_ROOT}/${spreadsheetId}:batchUpdate`, {
    method: "POST",
    body: JSON.stringify({
      requests: [
        {
          addSheet: {
            properties: {
              title: sheetName,
            },
          },
        },
      ],
    }),
  });

  const created =
    response?.replies?.[0]?.addSheet?.properties
      ? normaliseSheetProperties(response.replies[0].addSheet.properties)
      : null;
  if (!created) {
    throw new Error(`Google Sheets did not create the ${sheetName} tab.`);
  }
  upsertSheetPropertiesInCache(spreadsheetId, sheetName, created);
  return created;
}

async function ensureSheetCapacity({
  spreadsheetId,
  sheetName,
  properties,
  rowsNeeded,
  columnsNeeded,
}) {
  const rowCount = Math.max(properties.rowCount, rowsNeeded);
  const columnCount = Math.max(properties.columnCount, columnsNeeded);
  if (rowCount === properties.rowCount && columnCount === properties.columnCount) {
    return properties;
  }

  await googleApiJson(`${GOOGLE_SHEETS_API_ROOT}/${spreadsheetId}:batchUpdate`, {
    method: "POST",
    body: JSON.stringify({
      requests: [
        {
          updateSheetProperties: {
            properties: {
              sheetId: properties.sheetId,
              gridProperties: {
                rowCount,
                columnCount,
              },
            },
            fields: "gridProperties.rowCount,gridProperties.columnCount",
          },
        },
      ],
    }),
  });

  const nextProperties = {
    sheetId: properties.sheetId,
    rowCount,
    columnCount,
  };
  upsertSheetPropertiesInCache(spreadsheetId, sheetName, nextProperties);
  return nextProperties;
}

async function clearSheetValues(spreadsheetId, sheetName) {
  const range = encodeURIComponent(`'${sheetName}'`);
  await googleApiJson(`${GOOGLE_SHEETS_API_ROOT}/${spreadsheetId}/values/${range}:clear`, {
    method: "POST",
    body: JSON.stringify({}),
  });
}

async function updateSheetValues(spreadsheetId, sheetName, matrix) {
  const range = encodeURIComponent(`'${sheetName}'!A1`);
  await googleApiJson(
    `${GOOGLE_SHEETS_API_ROOT}/${spreadsheetId}/values/${range}?valueInputOption=RAW`,
    {
      method: "PUT",
      body: JSON.stringify({ values: matrix }),
    },
  );
}

async function applyRowBackgrounds({ spreadsheetId, properties, rowBackgrounds, columnCount }) {
  const requests = [
    {
      repeatCell: {
        range: {
          sheetId: properties.sheetId,
          startRowIndex: 1,
          endRowIndex: properties.rowCount,
          startColumnIndex: 0,
          endColumnIndex: columnCount,
        },
        cell: {
          userEnteredFormat: {
            backgroundColor: hexToGoogleColor(WHITE_BACKGROUND),
          },
        },
        fields: "userEnteredFormat.backgroundColor",
      },
    },
  ];

  for (const rowRange of buildColoredRowRanges(rowBackgrounds)) {
    requests.push({
      repeatCell: {
        range: {
          sheetId: properties.sheetId,
          startRowIndex: rowRange.startRowNumber - 1,
          endRowIndex: rowRange.endRowNumber,
          startColumnIndex: 0,
          endColumnIndex: columnCount,
        },
        cell: {
          userEnteredFormat: {
            backgroundColor: hexToGoogleColor(rowRange.color),
          },
        },
        fields: "userEnteredFormat.backgroundColor",
      },
    });
  }

  await googleApiJson(`${GOOGLE_SHEETS_API_ROOT}/${spreadsheetId}:batchUpdate`, {
    method: "POST",
    body: JSON.stringify({ requests }),
  });
}

function buildColoredRowRanges(rowBackgrounds) {
  const sortedRows = Object.entries(rowBackgrounds)
    .map(([rowNumber, color]) => ({
      rowNumber: Number(rowNumber),
      color,
    }))
    .filter((entry) => Number.isInteger(entry.rowNumber) && entry.rowNumber > 0)
    .sort((left, right) => left.rowNumber - right.rowNumber);

  if (sortedRows.length === 0) {
    return [];
  }

  const ranges = [];
  let currentRange = {
    startRowNumber: sortedRows[0].rowNumber,
    endRowNumber: sortedRows[0].rowNumber,
    color: sortedRows[0].color,
  };

  for (const entry of sortedRows.slice(1)) {
    const continuesCurrentRange =
      entry.color === currentRange.color &&
      entry.rowNumber === currentRange.endRowNumber + 1;
    if (continuesCurrentRange) {
      currentRange.endRowNumber = entry.rowNumber;
      continue;
    }

    ranges.push(currentRange);
    currentRange = {
      startRowNumber: entry.rowNumber,
      endRowNumber: entry.rowNumber,
      color: entry.color,
    };
  }

  ranges.push(currentRange);
  return ranges;
}

async function fetchSpreadsheetMetadata(spreadsheetId) {
  return googleApiJson(
    `${GOOGLE_SHEETS_API_ROOT}/${spreadsheetId}?fields=sheets(properties(sheetId,title,gridProperties(rowCount,columnCount)))`,
    {
      method: "GET",
    },
  );
}

async function fetchSpreadsheetMetadataCached(spreadsheetId, options = {}) {
  const cachedMetadata = state.spreadsheetMetadataCache.get(spreadsheetId);
  if (cachedMetadata && !options.forceRefresh) {
    return cachedMetadata;
  }

  const metadata = await fetchSpreadsheetMetadata(spreadsheetId);
  primeSpreadsheetMetadataCache(spreadsheetId, metadata);
  return metadata;
}

function primeSpreadsheetMetadataCache(spreadsheetId, metadata) {
  if (!metadata) {
    state.spreadsheetMetadataCache.delete(spreadsheetId);
    return;
  }
  state.spreadsheetMetadataCache.set(spreadsheetId, metadata);
}

function upsertSheetPropertiesInCache(spreadsheetId, sheetName, properties) {
  const cachedMetadata = state.spreadsheetMetadataCache.get(spreadsheetId);
  if (!cachedMetadata) {
    return;
  }

  const sheets = Array.isArray(cachedMetadata.sheets) ? [...cachedMetadata.sheets] : [];
  const nextProperties = {
    sheetId: properties.sheetId,
    title: sheetName,
    gridProperties: {
      rowCount: properties.rowCount,
      columnCount: properties.columnCount,
    },
  };
  const existingIndex = sheets.findIndex((sheet) => sheet?.properties?.title === sheetName);
  if (existingIndex >= 0) {
    sheets[existingIndex] = { properties: nextProperties };
  } else {
    sheets.push({ properties: nextProperties });
  }
  state.spreadsheetMetadataCache.set(spreadsheetId, { sheets });
}

function findSheetProperties(metadata, sheetName) {
  const sheets = Array.isArray(metadata.sheets) ? metadata.sheets : [];
  for (const sheet of sheets) {
    const properties = sheet?.properties;
    if (properties?.title === sheetName) {
      return normaliseSheetProperties(properties);
    }
  }
  return null;
}

function normaliseSheetProperties(properties) {
  return {
    sheetId: Number(properties.sheetId),
    rowCount: Number(properties.gridProperties?.rowCount ?? 1000),
    columnCount: Number(properties.gridProperties?.columnCount ?? 26),
  };
}

function normaliseRow(row, width) {
  const rawCells = Array.isArray(row) ? row.map(toStringCell) : [];
  const paddedCells = [...rawCells];
  while (paddedCells.length < width) {
    paddedCells.push("");
  }
  return paddedCells.slice(0, width);
}

function toStringCell(value) {
  return typeof value === "string" ? value : String(value ?? "");
}

function toNonEmptyString(value) {
  return typeof value === "string" && value.trim() ? value.trim() : "";
}

function hexToGoogleColor(hexColor) {
  const normalized = hexColor.replace("#", "");
  return {
    red: Number.parseInt(normalized.slice(0, 2), 16) / 255,
    green: Number.parseInt(normalized.slice(2, 4), 16) / 255,
    blue: Number.parseInt(normalized.slice(4, 6), 16) / 255,
  };
}

async function postJson(path, payload) {
  return fetchJson(path, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

async function fetchJson(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers ?? {}),
    },
  });
  const body = await parseJsonResponse(response);
  if (!response.ok) {
    throw new Error(body?.error ?? `Request failed with status ${response.status}.`);
  }
  return body;
}

async function googleApiJson(url, options) {
  const token = state.accessToken;
  const response = await fetch(url, {
    ...options,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(options.headers ?? {}),
    },
  });
  const body = await parseJsonResponse(response);
  if (response.status === 401) {
    clearGoogleSession();
    setSheetStatus(
      "Google access expired. Choose spreadsheet or connect Google again.",
      "warning",
    );
    updateActionAvailability();
    throw new Error("Google access expired. Choose spreadsheet or connect Google again.");
  }
  if (response.status === 403) {
    throw new Error(
      body?.error?.message ??
        "Google denied access to this spreadsheet. Choose it again from Google Drive and retry.",
    );
  }
  if (!response.ok) {
    throw new Error(
      body?.error?.message ??
        body?.error ??
        `Google API request failed with status ${response.status}.`,
    );
  }
  return body ?? {};
}

async function parseJsonResponse(response) {
  const text = await response.text();
  if (!text) {
    return null;
  }
  try {
    return JSON.parse(text);
  } catch {
    return { error: text };
  }
}

function readLocalStorage(key) {
  try {
    return window.localStorage.getItem(key);
  } catch {
    return null;
  }
}

function writeLocalStorage(key, value) {
  try {
    if (!value) {
      window.localStorage.removeItem(key);
      return;
    }
    window.localStorage.setItem(key, value);
  } catch {
    // Ignore storage failures in private or restricted browser contexts.
  }
}

function readSessionStorage(key) {
  try {
    return window.sessionStorage.getItem(key);
  } catch {
    return null;
  }
}

function writeSessionStorage(key, value) {
  try {
    if (!value) {
      window.sessionStorage.removeItem(key);
      return;
    }
    window.sessionStorage.setItem(key, value);
  } catch {
    // Ignore storage failures in private or restricted browser contexts.
  }
}

function initializeTooltips() {
  state.tooltipElement = elements.tooltip;
  if (!state.tooltipElement) {
    return;
  }

  const tooltipTargets = Array.from(document.querySelectorAll("[data-tooltip]"));
  tooltipTargets.forEach((element) => {
    element.addEventListener("mouseenter", (event) => {
      showTooltip(event, element);
    });
    element.addEventListener("mousemove", (event) => {
      positionTooltip(event.clientX, event.clientY);
    });
    element.addEventListener("mouseleave", hideTooltip);
    element.addEventListener("focus", () => {
      const rect = element.getBoundingClientRect();
      showTooltip(
        {
          clientX: rect.left + rect.width / 2,
          clientY: rect.top + rect.height / 2,
        },
        element,
      );
    });
    element.addEventListener("blur", hideTooltip);
  });

  window.addEventListener("scroll", hideTooltip, { passive: true });
  window.addEventListener("resize", hideTooltip);
}

function positionTooltip(clientX, clientY) {
  if (!state.tooltipElement) {
    return;
  }

  const offset = 14;
  const padding = 16;
  const rect = state.tooltipElement.getBoundingClientRect();
  let left = clientX + offset;
  let top = clientY + offset;

  if (left + rect.width > window.innerWidth - padding) {
    left = window.innerWidth - rect.width - padding;
  }

  if (top + rect.height > window.innerHeight - padding) {
    top = clientY - rect.height - offset;
  }

  state.tooltipElement.style.transform = `translate(${left}px, ${top}px)`;
}

function showTooltip(event, element) {
  if (!state.tooltipElement || element.disabled) {
    return;
  }

  state.tooltipElement.textContent = element.dataset.tooltip;
  state.tooltipElement.classList.add("is-visible");
  state.tooltipElement.setAttribute("aria-hidden", "false");
  positionTooltip(event.clientX, event.clientY);
}

function hideTooltip() {
  if (!state.tooltipElement) {
    return;
  }

  state.tooltipElement.classList.remove("is-visible");
  state.tooltipElement.setAttribute("aria-hidden", "true");
  state.tooltipElement.style.transform = "translate(-9999px, -9999px)";
}

function sleep(milliseconds) {
  return new Promise((resolve) => {
    window.setTimeout(resolve, milliseconds);
  });
}
