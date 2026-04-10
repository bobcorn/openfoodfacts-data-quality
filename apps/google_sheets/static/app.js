const DRIVE_FILE_SCOPE = "https://www.googleapis.com/auth/drive.file";
const GOOGLE_SHEETS_API_ROOT = "https://sheets.googleapis.com/v4/spreadsheets";
const WHITE_BACKGROUND = "#FFFFFF";
const SELECTED_SPREADSHEET_STORAGE_KEY =
  "off-google-sheets:selected-spreadsheet";
const GOOGLE_SESSION_STORAGE_KEY =
  "off-google-sheets:google-session";
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
  csvPreview: {
    fileKey: "",
    status: "idle",
    parsed: null,
    errorMessage: "",
  },
  csvPreviewRequestId: 0,
  validationProbe: {
    key: "",
    status: "idle",
    hasValidationOutput: false,
  },
};

const elements = {
  connectButton: document.querySelector("#connect-button"),
  chooseSpreadsheetButton: document.querySelector("#choose-spreadsheet-button"),
  openSpreadsheetButton: document.querySelector("#open-spreadsheet-button"),
  clearSpreadsheetButton: document.querySelector("#clear-spreadsheet-button"),
  selectedSheetActions: document.querySelector("#selected-sheet-actions"),
  selectedSheetName: document.querySelector("#selected-sheet-name"),
  selectedSheetMeta: document.querySelector("#selected-sheet-meta"),
  inputSheetNameInput: document.querySelector("#input-sheet-name"),
  outputSheetNameInput: document.querySelector("#output-sheet-name"),
  sheetStatus: document.querySelector("#sheet-status"),
  csvFileInput: document.querySelector("#csv-file-input"),
  dataFileStatus: document.querySelector("#data-file-status"),
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
  renderSheetNameReferences();
  renderSelectedSpreadsheet();
  resetSummary();
  await configureGoogleClients();
  updateActionAvailability();
}

function bindEvents() {
  elements.inputSheetNameInput.addEventListener("input", () => {
    if (state.uploadCandidatesPrepared) {
      resetUploadCandidatesPrepared();
    }
    resetValidationProbe();
    renderSheetNameReferences();
    renderSelectedSpreadsheet();
    updateActionAvailability();
  });
  elements.outputSheetNameInput.addEventListener("input", () => {
    if (state.uploadCandidatesPrepared) {
      resetUploadCandidatesPrepared();
    }
    renderSheetNameReferences();
    renderSelectedSpreadsheet();
    updateActionAvailability();
  });
  elements.connectButton.addEventListener("click", () => {
    if (isButtonUnavailable(elements.connectButton)) {
      return;
    }
    const action = hasUsableGoogleSession() ? handleDisconnect : handleLogin;
    const pendingMessage = hasUsableGoogleSession()
      ? "Disconnecting Google session..."
      : "Opening Google sign in...";
    void runWithUiLock(action, {
      statusElement: elements.sheetStatus,
      pendingMessage,
    });
  });
  elements.chooseSpreadsheetButton.addEventListener("click", () => {
    if (isButtonUnavailable(elements.chooseSpreadsheetButton)) {
      return;
    }
    void runWithUiLock(handleChooseSpreadsheet, {
      statusElement: elements.sheetStatus,
      pendingMessage: "Opening Google Drive Picker...",
    });
  });
  elements.openSpreadsheetButton.addEventListener("click", () => {
    if (isButtonUnavailable(elements.openSpreadsheetButton)) {
      return;
    }
    const url = currentSpreadsheetUrl();
    window.open(url, "_blank", "noopener,noreferrer");
  });
  elements.clearSpreadsheetButton.addEventListener("click", () => {
    if (isButtonUnavailable(elements.clearSpreadsheetButton)) {
      return;
    }
    void runWithUiLock(handleClearSpreadsheetSelection, {
      statusElement: elements.sheetStatus,
      pendingMessage: "Clearing spreadsheet selection...",
    });
  });
  elements.uploadCsvButton.addEventListener("click", () => {
    if (isButtonUnavailable(elements.uploadCsvButton)) {
      return;
    }
    void runWithUiLock(handleUploadCsv, {
      statusElement: elements.dataStatus,
      pendingMessage: "Preparing CSV upload...",
    });
  });
  elements.csvFileInput.addEventListener("change", () => {
    void handleCsvSelectionChange();
  });
  elements.validateButton.addEventListener("click", () => {
    if (isButtonUnavailable(elements.validateButton)) {
      return;
    }
    void runWithUiLock(handleValidate, {
      statusElement: elements.actionStatus,
      pendingMessage: "Preparing validation...",
    });
  });
  elements.clearButton.addEventListener("click", () => {
    if (isButtonUnavailable(elements.clearButton)) {
      return;
    }
    void runWithUiLock(handleClearValidationOutput, {
      statusElement: elements.actionStatus,
      pendingMessage: "Preparing clear action...",
    });
  });
  elements.prepareButton.addEventListener("click", () => {
    if (isButtonUnavailable(elements.prepareButton)) {
      return;
    }
    void runWithUiLock(handlePrepareUploadCandidates, {
      statusElement: elements.actionStatus,
      pendingMessage: "Preparing candidate rows...",
    });
  });
  elements.uploadOffButton.addEventListener("click", () => {
    if (isButtonUnavailable(elements.uploadOffButton)) {
      return;
    }
    void runWithUiLock(handleUploadToOpenFoodFacts, {
      statusElement: elements.actionStatus,
      pendingMessage: "Checking upload readiness...",
    });
  });
}

async function loadPublicConfig() {
  const config = await fetchJson("/api/config");
  state.config = config;
  elements.inputSheetNameInput.value = config.inputSheetName ?? "Data";
  elements.outputSheetNameInput.value =
    config.outputSheetName ?? "Ready for OFF upload";
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
    setSheetStatus("Google login is not available right now.", "warning");
    renderSelectedSpreadsheet();
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
      "Google Picker is not available right now.",
      "warning",
    );
    renderSelectedSpreadsheet();
    return;
  }

  await loadGooglePickerLibrary();
  const restoredSession = restoreGoogleSession();
  if (!restoredSession) {
    clearSelectedSpreadsheet();
  }
  clearStatus(elements.sheetStatus);
  renderSelectedSpreadsheet();
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
  const hasPickerConfigurationValues = hasPickerConfiguration();
  const pickerReady = hasPickerConfigurationValues && state.pickerLoaded;
  const hasAuth = hasUsableGoogleSession();
  const hasSpreadsheet = Boolean(state.selectedSpreadsheet?.id);
  const flowReady = hasSpreadsheet && hasAuth;
  const actionsEnabled = !state.busy && flowReady;
  const canPrepare = actionsEnabled && hasAvailableValidationOutput();

  setExplainableButtonState(elements.connectButton, {
    interactive: !state.busy && (hasAuth || hasAuthConfiguration),
    hardDisabled: false,
    activeTooltip: hasAuth
      ? "Clear the Google session from this browser tab."
      : "Open Google sign in for this browser tab.",
    blockedTooltip: !hasAuth && !hasAuthConfiguration
      ? "Google login is not available right now."
      : "Wait until the current step finishes.",
  });
  elements.connectButton.textContent = hasAuth
    ? "Disconnect Google"
    : "Connect Google";
  elements.connectButton.classList.toggle("button--primary", !hasAuth);
  elements.connectButton.classList.toggle("button--secondary", hasAuth);
  elements.connectButton.classList.toggle("button--quiet", false);

  const canChooseSpreadsheet =
    !state.busy && !hasSpreadsheet && hasAuthConfiguration && pickerReady && hasAuth;
  setExplainableButtonState(elements.chooseSpreadsheetButton, {
    interactive: canChooseSpreadsheet,
    hardDisabled: false,
    activeTooltip: "Open Google Picker and choose one spreadsheet.",
    blockedTooltip: chooseSpreadsheetBlockedTooltip({
      hasAuthConfiguration,
      hasPickerConfigurationValues,
      pickerReady,
      hasAuth,
    }),
  });
  elements.chooseSpreadsheetButton.textContent = "Choose spreadsheet";
  elements.chooseSpreadsheetButton.hidden = hasSpreadsheet;
  elements.chooseSpreadsheetButton.classList.toggle(
    "button--primary",
    hasAuth,
  );
  elements.chooseSpreadsheetButton.classList.toggle(
    "button--secondary",
    !hasAuth,
  );
  elements.chooseSpreadsheetButton.classList.remove("button--quiet");
  elements.selectedSheetActions.hidden = !hasSpreadsheet;
  setExplainableButtonState(elements.openSpreadsheetButton, {
    interactive: hasSpreadsheet,
    activeTooltip: "Open the selected spreadsheet directly in Google Sheets.",
    blockedTooltip: "Choose a spreadsheet first.",
  });
  setExplainableButtonState(elements.clearSpreadsheetButton, {
    interactive: !state.busy && hasSpreadsheet,
    activeTooltip: "Forget the current spreadsheet selection in this browser.",
    blockedTooltip: "Choose a spreadsheet first.",
  });
  updateFlowStepVisibility(flowReady);

  const canUploadCsv = actionsEnabled && state.csvPreview.status === "ready";
  setExplainableButtonState(elements.uploadCsvButton, {
    interactive: canUploadCsv,
    hardDisabled: false,
    activeTooltip:
      `Replace the ${describeGoogleSheetsTab(currentInputSheetName())} ` +
      "with the selected CSV file.",
    blockedTooltip: uploadCsvBlockedTooltip(),
  });
  setExplainableButtonState(elements.validateButton, {
    interactive: actionsEnabled,
    hardDisabled: false,
    activeTooltip:
      "Run Python checks. The app writes dq_* columns and row colors.",
    blockedTooltip: "Wait until the current step finishes.",
  });
  setExplainableButtonState(elements.clearButton, {
    interactive: actionsEnabled,
    hardDisabled: false,
    activeTooltip: "Remove dq_* columns and clear row formatting.",
    blockedTooltip: "Wait until the current step finishes.",
  });
  setExplainableButtonState(elements.prepareButton, {
    interactive: canPrepare,
    hardDisabled: false,
    activeTooltip:
      `Copy rows with dq_status = ok into ` +
      `${describeGoogleSheetsTab(currentOutputSheetName())} ` +
      "and remove dq_* columns.",
    blockedTooltip: prepareUploadBlockedTooltip({ actionsEnabled }),
  });
  const canAttemptOffUpload = actionsEnabled && state.uploadCandidatesPrepared;
  setExplainableButtonState(elements.uploadOffButton, {
    interactive: canAttemptOffUpload,
    hardDisabled: false,
    activeTooltip: "Upload the prepared rows to Open Food Facts.",
    blockedTooltip: "Run Prepare upload candidates first.",
  });
  elements.validateButton.classList.toggle("button--primary", !canPrepare);
  elements.validateButton.classList.toggle("button--secondary", canPrepare);
  elements.prepareButton.classList.toggle("button--primary", canPrepare);
  elements.prepareButton.classList.toggle("button--secondary", !canPrepare);
  renderSelectedSpreadsheet();
  if (flowReady) {
    void refreshValidationProbe();
    return;
  }
  resetValidationProbe();
}

function updateFlowStepVisibility(flowReady) {
  toggleFlowStep(elements.loadPanel, flowReady);
  toggleFlowStep(elements.actionsPanel, flowReady);
  toggleFlowStep(elements.summaryPanel, flowReady);
}

function toggleFlowStep(panel, visible) {
  panel.classList.toggle("workflow-step--hidden", !visible);
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
  if (!interactive) {
    clearGoogleSession();
    throw new Error("Google session expired. Connect Google again.");
  }
  if (!state.tokenClient) {
    throw new Error("Google login is not available right now.");
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
  await ensureGoogleAccessToken({ interactive: false });
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
    throw new Error("Google Picker is not available right now.");
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
  resetValidationProbe();
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
  resetValidationProbe();
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
    elements.selectedSheetName.textContent = "No spreadsheet selected yet.";
    elements.selectedSheetMeta.textContent = hasUsableGoogleSession()
      ? "Choose a Sheet from Google Drive to enable the demo."
      : "Connect Google, then choose a Sheet to enable the demo.";
    return;
  }

  elements.selectedSheetName.textContent = selection.name;
  elements.selectedSheetMeta.textContent =
    `Input Google Sheets tab: ${currentInputSheetName()} | ` +
    `Output Google Sheets tab: ${currentOutputSheetName()}`;
}

async function handleUploadCsv() {
  const spreadsheetId = currentSpreadsheetId();
  const file = currentSelectedCsvFile();
  if (!file) {
    throw new Error("Choose a CSV file before uploading.");
  }

  await ensureGoogleAccessToken({ interactive: false });
  const parsed = await ensureParsedSelectedCsvFile(file);
  const inputSheetName = currentInputSheetName();
  setDataStatus(
    `Uploading ${parsed.table.rows.length} rows into ` +
      `${describeGoogleSheetsTab(inputSheetName)}...`,
    "neutral",
    {
      loading: true,
    },
  );
  await replaceSheetTable({
    spreadsheetId,
    sheetName: inputSheetName,
    table: parsed.table,
    rowBackgrounds: {},
  });
  resetUploadCandidatesPrepared();
  setValidationProbeAvailability(false);
  resetSummary();
  const uploadStatus = buildCsvUploadStatus(parsed, inputSheetName);
  setDataStatus(uploadStatus.message, uploadStatus.kind);
}

async function handleValidate() {
  const spreadsheetId = currentSpreadsheetId();
  await ensureGoogleAccessToken({ interactive: false });
  const inputSheetName = currentInputSheetName();
  setActionStatus(`Reading ${describeGoogleSheetsTab(inputSheetName)}...`, "neutral", {
    loading: true,
  });
  const { table, sheetProperties } = await readSheetTable(
    spreadsheetId,
    inputSheetName,
  );
  if (!sheetProperties) {
    throw new Error(
      `Upload a CSV or populate ${describeGoogleSheetsTab(inputSheetName)} before running Validate data.`,
    );
  }
  setActionStatus("Running Python checks on the current rows...", "neutral", {
    loading: true,
  });
  const response = await postJson("/api/validate", { table });
  setActionStatus("Writing dq_* columns and row colors to the sheet...", "neutral", {
    loading: true,
  });
  await replaceSheetTable({
    spreadsheetId,
    sheetName: inputSheetName,
    table: response.table,
    rowBackgrounds: response.rowBackgrounds ?? {},
    sheetProperties,
  });
  resetUploadCandidatesPrepared();
  setValidationProbeAvailability(true);
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
  const inputSheetName = currentInputSheetName();
  setActionStatus(`Reading ${describeGoogleSheetsTab(inputSheetName)}...`, "neutral", {
    loading: true,
  });
  const { table, sheetProperties } = await readSheetTable(
    spreadsheetId,
    inputSheetName,
  );
  if (!sheetProperties) {
    throw new Error(
      `${describeGoogleSheetsTab(inputSheetName)} does not exist yet.`,
    );
  }
  const clearedTable = stripValidationColumnsInBrowser(table);
  setActionStatus("Removing dq_* columns and clearing row formatting...", "neutral", {
    loading: true,
  });
  await replaceSheetTable({
    spreadsheetId,
    sheetName: inputSheetName,
    table: clearedTable,
    rowBackgrounds: {},
    sheetProperties,
  });
  resetUploadCandidatesPrepared();
  setValidationProbeAvailability(false);
  resetSummary();
  setActionStatus(
    `Removed derived validation columns from ` +
      `${describeGoogleSheetsTab(inputSheetName)}.`,
    "success",
  );
}

async function handlePrepareUploadCandidates() {
  if (!hasAvailableValidationOutput()) {
    setActionStatus(
      "Run Validate data before preparing " +
        `${describeGoogleSheetsTab(currentOutputSheetName())}.`,
      "warning",
    );
    return;
  }
  const spreadsheetId = currentSpreadsheetId();
  await ensureGoogleAccessToken({ interactive: false });
  const inputSheetName = currentInputSheetName();
  const outputSheetName = currentOutputSheetName();
  setActionStatus(
    `Reading rows from ${describeGoogleSheetsTab(inputSheetName)}...`,
    "neutral",
    {
      loading: true,
    },
  );
  const { table, sheetProperties } = await readSheetTable(
    spreadsheetId,
    inputSheetName,
  );
  if (!sheetProperties) {
    throw new Error(
      `Upload a CSV or populate ${describeGoogleSheetsTab(inputSheetName)} before preparing upload candidates.`,
    );
  }
  const candidateTable = prepareUploadCandidatesInBrowser(table);
  setActionStatus(
    `Writing only clean rows into ` +
      `${describeGoogleSheetsTab(outputSheetName)}...`,
    "neutral",
    {
      loading: true,
    },
  );
  await replaceSheetTable({
    spreadsheetId,
    sheetName: outputSheetName,
    table: candidateTable,
    rowBackgrounds: {},
  });
  state.uploadCandidatesPrepared = true;
  updateActionAvailability();
  setActionStatus(
    `Prepared ${candidateTable.rows.length} upload candidates in ` +
      `${describeGoogleSheetsTab(outputSheetName)}.`,
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

function setDataFileStatus(message, kind, options) {
  setStatus(elements.dataFileStatus, message, kind, options);
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

function buildCsvUploadStatus(parsed, inputSheetName) {
  return {
    kind: "success",
    message:
      `Loaded ${parsed.table.rows.length} rows into ` +
      `${describeGoogleSheetsTab(inputSheetName)}.`,
  };
}

function buildCsvPreviewStatus(parsed) {
  const schema = parsed?.schema;
  if (
    !Number.isFinite(schema?.recognizedColumnsCount) ||
    !Number.isFinite(schema?.supportedColumnsCount)
  ) {
    return "";
  }
  return (
    `This file includes ${schema.recognizedColumnsCount} of the ` +
    `${schema.supportedColumnsCount} canonical columns currently supported by the library. ` +
    "The checks will run on the supported columns that are present in the provided file."
  );
}

function renderSheetNameReferences() {
  const inputSheetName = currentInputSheetName();
  const outputSheetName = currentOutputSheetName();
  for (const element of document.querySelectorAll("[data-sheet-name-ref='input']")) {
    element.textContent = inputSheetName;
  }
  for (const element of document.querySelectorAll("[data-sheet-name-ref='output']")) {
    element.textContent = outputSheetName;
  }
}

function describeGoogleSheetsTab(sheetName) {
  return `Google Sheets tab "${sheetName}"`;
}

function validationProbeKey() {
  const spreadsheetId = state.selectedSpreadsheet?.id;
  if (!spreadsheetId) {
    return "";
  }
  return `${spreadsheetId}:${currentInputSheetName()}`;
}

function resetValidationProbe() {
  state.validationProbe = {
    key: "",
    status: "idle",
    hasValidationOutput: false,
  };
}

function setValidationProbeAvailability(hasValidationOutput) {
  state.validationProbe = {
    key: validationProbeKey(),
    status: "ready",
    hasValidationOutput,
  };
}

function hasAvailableValidationOutput() {
  return (
    state.validationProbe.status === "ready" &&
    state.validationProbe.hasValidationOutput
  );
}

async function refreshValidationProbe() {
  const key = validationProbeKey();
  if (!key || state.busy || !hasUsableGoogleSession()) {
    return;
  }
  if (
    state.validationProbe.key === key &&
    (state.validationProbe.status === "loading" ||
      state.validationProbe.status === "ready")
  ) {
    return;
  }

  state.validationProbe = {
    key,
    status: "loading",
    hasValidationOutput: false,
  };
  updateActionAvailability();
  try {
    const { table, sheetProperties } = await readSheetTable(
      currentSpreadsheetId(),
      currentInputSheetName(),
    );
    if (validationProbeKey() !== key) {
      return;
    }
    state.validationProbe = {
      key,
      status: "ready",
      hasValidationOutput:
        sheetProperties !== null && table.headers.includes("dq_status"),
    };
  } catch {
    if (validationProbeKey() !== key) {
      return;
    }
    state.validationProbe = {
      key,
      status: "error",
      hasValidationOutput: false,
    };
  }
  updateActionAvailability();
}

function hasUsableGoogleSession() {
  return Boolean(state.accessToken) && Date.now() < state.tokenExpiresAt - 30_000;
}

function restoreGoogleSession() {
  restoreStoredGoogleSession();
  if (hasUsableGoogleSession()) {
    return true;
  }
  clearGoogleSession();
  return false;
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

function currentInputSheetName() {
  const value = elements.inputSheetNameInput.value.trim();
  return value || "Data";
}

function currentOutputSheetName() {
  const value = elements.outputSheetNameInput.value.trim();
  return value || "Ready for OFF upload";
}

function currentSelectedCsvFile() {
  return elements.csvFileInput.files?.[0] ?? null;
}

function hasSelectedCsvFile() {
  return Boolean(currentSelectedCsvFile());
}

function csvFileCacheKey(file) {
  return `${file.name}:${file.size}:${file.lastModified}`;
}

function uploadCsvBlockedTooltip() {
  if (!hasSelectedCsvFile()) {
    return "Choose a CSV file first.";
  }
  if (state.csvPreview.status === "loading") {
    return "Wait until the selected CSV is checked.";
  }
  if (state.csvPreview.status === "error") {
    return state.csvPreview.errorMessage || "Choose a valid CSV file first.";
  }
  return (
    `Replace the ${describeGoogleSheetsTab(currentInputSheetName())} ` +
    "with the selected CSV file."
  );
}

function prepareUploadBlockedTooltip({ actionsEnabled }) {
  if (!actionsEnabled) {
    return "Wait until the current step finishes.";
  }
  if (state.validationProbe.status === "loading") {
    return (
      `Checking whether ${describeGoogleSheetsTab(currentInputSheetName())} ` +
      "already has validation output."
    );
  }
  if (state.validationProbe.status === "error") {
    return (
      `Validate ${describeGoogleSheetsTab(currentInputSheetName())} ` +
      `before preparing ${describeGoogleSheetsTab(currentOutputSheetName())}.`
    );
  }
  return (
    "Run Validate data before preparing " +
    `${describeGoogleSheetsTab(currentOutputSheetName())}.`
  );
}

function resetCsvPreview() {
  state.csvPreview = {
    fileKey: "",
    status: "idle",
    parsed: null,
    errorMessage: "",
  };
}

async function handleCsvSelectionChange() {
  clearStatus(elements.dataStatus);
  resetCsvPreview();
  state.csvPreviewRequestId += 1;
  const requestId = state.csvPreviewRequestId;
  const file = currentSelectedCsvFile();
  if (!file) {
    clearStatus(elements.dataFileStatus);
    updateActionAvailability();
    return;
  }

  const fileKey = csvFileCacheKey(file);
  state.csvPreview.fileKey = fileKey;
  state.csvPreview.status = "loading";
  updateActionAvailability();
  setDataFileStatus("Checking the selected CSV...", "neutral", {
    loading: true,
  });

  try {
    const csvText = await file.text();
    if (requestId !== state.csvPreviewRequestId) {
      return;
    }
    const parsed = await postJson("/api/parse-csv", {
      fileName: file.name,
      csvText,
    });
    if (requestId !== state.csvPreviewRequestId) {
      return;
    }
    state.csvPreview = {
      fileKey,
      status: "ready",
      parsed,
      errorMessage: "",
    };
    setDataFileStatus(buildCsvPreviewStatus(parsed), "neutral");
  } catch (error) {
    if (requestId !== state.csvPreviewRequestId) {
      return;
    }
    const message = error instanceof Error ? error.message : String(error);
    state.csvPreview = {
      fileKey,
      status: "error",
      parsed: null,
      errorMessage: message,
    };
    setDataFileStatus(message, "error");
  } finally {
    if (requestId === state.csvPreviewRequestId) {
      updateActionAvailability();
    }
  }
}

async function ensureParsedSelectedCsvFile(file) {
  const fileKey = csvFileCacheKey(file);
  if (state.csvPreview.fileKey === fileKey) {
    if (state.csvPreview.status === "ready" && state.csvPreview.parsed) {
      return state.csvPreview.parsed;
    }
    if (state.csvPreview.status === "loading") {
      throw new Error("Wait until the selected CSV is checked.");
    }
    if (state.csvPreview.status === "error") {
      throw new Error(
        state.csvPreview.errorMessage || "Choose a valid CSV file before uploading.",
      );
    }
  }

  const csvText = await file.text();
  const parsed = await postJson("/api/parse-csv", {
    fileName: file.name,
    csvText,
  });
  state.csvPreview = {
    fileKey,
    status: "ready",
    parsed,
    errorMessage: "",
  };
  setDataFileStatus(buildCsvPreviewStatus(parsed), "neutral");
  updateActionAvailability();
  return parsed;
}

function chooseSpreadsheetBlockedTooltip({
  hasAuthConfiguration,
  hasPickerConfigurationValues,
  pickerReady,
  hasAuth,
}) {
  if (!hasAuthConfiguration) {
    return "Google login is not available right now.";
  }
  if (!hasAuth) {
    return "Connect Google first.";
  }
  if (!hasPickerConfigurationValues) {
    return "Google Picker is not available right now.";
  }
  if (!pickerReady) {
    return "Google Picker is still loading.";
  }
  return "Open Google Picker and choose one spreadsheet.";
}

function setExplainableButtonState(
  element,
  { interactive, hardDisabled = false, activeTooltip, blockedTooltip },
) {
  element.disabled = hardDisabled;
  element.setAttribute(
    "aria-disabled",
    !hardDisabled && !interactive ? "true" : "false",
  );
  if (interactive) {
    element.dataset.tooltip = activeTooltip;
    return;
  }
  element.dataset.tooltip = blockedTooltip;
}

function isAriaDisabled(element) {
  return element.getAttribute("aria-disabled") === "true";
}

function isButtonUnavailable(element) {
  return element.disabled || isAriaDisabled(element);
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
    throw new Error(
      `Validate ${describeGoogleSheetsTab(currentInputSheetName())} ` +
      "before preparing upload candidates.",
    );
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
  const sheetProperties = await findExistingSheet(spreadsheetId, sheetName);
  if (!sheetProperties) {
    return {
      table: { headers: [], rows: [] },
      sheetProperties: null,
    };
  }
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

async function findExistingSheet(spreadsheetId, sheetName) {
  const metadata = await fetchSpreadsheetMetadataCached(spreadsheetId);
  return findSheetProperties(metadata, sheetName);
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
    throw new Error(
      `Google Sheets did not create ${describeGoogleSheetsTab(sheetName)}.`,
    );
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
      "Google access expired. Connect Google again to keep using this spreadsheet.",
      "warning",
    );
    updateActionAvailability();
    throw new Error(
      "Google access expired. Connect Google again to keep using this spreadsheet.",
    );
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
