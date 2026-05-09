(function () {
  "use strict";

  const state = {
    account: null,
    credentials: null,
    busy: false,
    firstAdminRequired: false,
    initialScanStarted: false,
    bootstrapStatus: null,
    setupLoaded: false,
    setupMode: "first-admin",
    systemConfig: null,
    providerSettings: new Map(),
    activeSection: "dashboard",
    summary: null,
    transcodeSlots: null,
    providerHealth: [],
    failures: [],
    failureFilterJobId: null,
    users: [],
    dashboardPollId: null,
    dashboardPolling: false,
  };

  const dashboardSections = ["dashboard", "failures", "providers", "users"];

  const providerControls = [
    {
      provider: "discogs",
      enabledId: "provider-discogs",
      keyId: "discogs-api-key",
    },
    {
      provider: "fanart_tv",
      enabledId: "provider-fanart",
      keyId: "fanart-api-key",
    },
    {
      provider: "the_audio_db",
      enabledId: "provider-audiodb",
      keyId: "audiodb-api-key",
    },
  ];

  document.addEventListener("DOMContentLoaded", () => {
    bindForms();
    bindDashboard();
    checkBootstrapStatus();
  });

  /**
   * Handles bind forms for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * Inputs: None.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function bindForms() {
    byId("setup-form").addEventListener("submit", runFirstSetup);
    byId("login-form").addEventListener("submit", login);
    byId("back-button").addEventListener("click", () => {
      window.scrollTo({ top: 0, behavior: "smooth" });
    });

    for (const control of providerControls) {
      const checkbox = byId(control.enabledId);
      const input = byId(control.keyId);
      input.addEventListener("input", () => {
        if (input.value.trim()) {
          checkbox.checked = true;
        }
      });
    }
  }

  /**
   * Handles bind dashboard for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * Inputs: None.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function bindDashboard() {
    for (const link of document.querySelectorAll(".dashboard nav a")) {
      link.addEventListener("click", (event) => {
        if (link.getAttribute("aria-disabled") === "true") {
          event.preventDefault();
          return;
        }
        const section = link.hash.slice(1);
        if (!dashboardSections.includes(section)) {
          return;
        }
        event.preventDefault();
        if (section === "failures") {
          state.failureFilterJobId = null;
        }
        window.history.replaceState(null, "", `/admin#${section}`);
        openDashboardSection(section);
      });
    }

    window.addEventListener("hashchange", () => {
      if (byId("dashboard-view").classList.contains("hidden")) {
        return;
      }
      openDashboardSection(sectionFromHash());
    });

    byId("refresh-dashboard-button").addEventListener("click", loadDashboardData);
    byId("refresh-failures-button").addEventListener("click", loadFailures);
    byId("clear-failures-filter-button").addEventListener("click", () => {
      state.failureFilterJobId = null;
      loadFailures();
    });
    byId("scan-progress-list").addEventListener("click", handleScanProgressClick);
    byId("refresh-providers-button").addEventListener("click", loadProviderHealth);
    byId("refresh-users-button").addEventListener("click", loadUsers);
    byId("full-rescan-button").addEventListener("click", triggerFullRescan);
    byId("subtree-rescan-form").addEventListener("submit", triggerSubtreeRescan);
    byId("create-user-form").addEventListener("submit", createUser);
    byId("users-table-body").addEventListener("click", handleUserTableClick);
  }

  /**
   * Handles check bootstrap status for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * Inputs: None.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function checkBootstrapStatus() {
    showOnly("setup-view");
    setSetupState("Checking setup");
    setSetupMessage("", "hidden");

    try {
      const status = await loadBootstrapStatus();
      if (status.first_admin_required) {
        openFirstAdminSetup();
        return;
      }

      showOnly("login-view");
    } catch (error) {
      setSetupState("Unavailable");
      setSetupMessage(error.message, "error");
    }
  }

  /**
   * Handles login for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} event - Expected to be a DOM event object.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function login(event) {
    event.preventDefault();
    if (state.busy) {
      return;
    }

    const username = byId("login-user").value.trim();
    const password = byId("login-pass").value;
    if (!username || !password) {
      setLoginMessage("Enter admin credentials.", "warning");
      return;
    }

    state.credentials = { username, password };
    setLoginMessage("Checking credentials...", "warning");
    setBusy(true);

    try {
      const account = await api("/api/v1/auth/me");
      if (account.role !== "admin") {
        throw new Error("This account is not an admin.");
      }
      state.account = account;
      await openSetupOrDashboard("Review saved setup settings, then start the initial scan.");
    } catch (error) {
      state.account = null;
      state.credentials = null;
      setLoginMessage(error.message, "error");
    } finally {
      setBusy(false);
    }
  }

  /**
   * Runs the operation for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} event - Expected to be a DOM event object.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function runFirstSetup(event) {
    event.preventDefault();
    if (state.busy) {
      return;
    }

    const username = byId("admin-user").value.trim();
    const password = byId("admin-pass").value;

    if (state.firstAdminRequired) {
      if (!username || !password) {
        setSetupMessage("Username and password are required.", "warning");
        return;
      }
    } else if (!state.credentials) {
      setSetupMessage("Sign in with an admin account before continuing setup.", "warning");
      return;
    }

    if (state.firstAdminRequired) {
      state.credentials = { username, password };
    }
    setBusy(true);
    setWizardBusy(true);
    clearSteps();

    try {
      if (state.firstAdminRequired) {
        await runStep("admin", "Creating first admin account...", () =>
          createFirstAdminOrUseExisting(username, password)
        );
        state.firstAdminRequired = false;
        state.setupMode = "resume";
        markStep("paths", "active");
        setSetupMessage("Loading saved setup settings...", "warning");
        await openSetupOrDashboard(
          "Admin account created. Review saved settings, then start the initial scan."
        );
        return;
      }

      if (!state.setupLoaded) {
        await loadSetupState();
      }

      const libraryRoot = byId("library-root").value.trim();
      const dropboxRoot = byId("dropbox-root").value.trim();

      if (!libraryRoot || !dropboxRoot) {
        setSetupMessage("Library root and dropbox root are required.", "warning");
        setWizardBusy(false);
        return;
      }

      markStep("admin", "complete");

      await runStep("paths", "Saving managed paths...", () =>
        saveSystemConfig(libraryRoot, dropboxRoot)
      );

      await runStep("providers", "Saving provider settings...", configureProviders);

      await runStep("scan", "Starting initial scan...", startInitialScan);

      setSetupMessage("Initial scan started. Redirecting to the dashboard.", "success");
      window.history.replaceState(null, "", "/admin#dashboard");
      window.setTimeout(openDashboard, 500);
    } catch (error) {
      markActiveStepError();
      setSetupMessage(error.message, "error");
    } finally {
      setBusy(false);
      setWizardBusy(false);
    }
  }

  /**
   * Opens a browser view or route for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} resumeMessage - Expected to be a value supplied by the caller or server response.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function openSetupOrDashboard(resumeMessage) {
    const status = await loadBootstrapStatus();
    if (status.initial_scan_started) {
      await openDashboard();
      return;
    }

    await loadSetupState();
    openResumableSetup(resumeMessage);
  }

  /**
   * Loads persisted state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * Inputs: None.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function loadBootstrapStatus() {
    const status = await api("/api/v1/bootstrap/status", { auth: false });
    state.bootstrapStatus = status;
    state.firstAdminRequired = Boolean(status.first_admin_required);
    state.initialScanStarted = Boolean(status.initial_scan_started);
    return {
      ...status,
      first_admin_required: state.firstAdminRequired,
      initial_scan_started: state.initialScanStarted,
    };
  }

  /**
   * Loads persisted state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * Inputs: None.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function loadSetupState() {
    const [systemConfig, providerSettings] = await Promise.all([
      api("/api/v1/admin/system/config"),
      api("/api/v1/admin/providers/settings"),
    ]);

    state.systemConfig = systemConfig;
    state.providerSettings = providerSettingsMap(providerSettings);
    state.setupLoaded = true;
    hydrateSystemConfig(systemConfig);
    hydrateProviderSettings();
  }

  /**
   * Handles provider settings map for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} response - Expected to be a value supplied by the caller or server response.
   * @returns {unknown} Returns the computed value described by the function body.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function providerSettingsMap(response) {
    const providers = response && Array.isArray(response.providers) ? response.providers : [];
    const settings = new Map();
    for (const provider of providers) {
      settings.set(provider.provider, provider);
    }
    return settings;
  }

  /**
   * Handles hydrate system config for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} config - Expected to be a value supplied by the caller or server response.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function hydrateSystemConfig(config) {
    byId("library-root").value = config && config.library_root ? config.library_root : "";
    byId("dropbox-root").value = config && config.dropbox_root ? config.dropbox_root : "";
  }

  /**
   * Handles hydrate provider settings for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * Inputs: None.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function hydrateProviderSettings() {
    const musicBrainz = state.providerSettings.get("music_brainz");
    const coverArt = state.providerSettings.get("cover_art_archive");
    const baseline = byId("provider-musicbrainz");
    const musicBrainzEnabled = Boolean(musicBrainz && musicBrainz.enabled);
    const coverArtEnabled = Boolean(coverArt && coverArt.enabled);
    baseline.checked = musicBrainzEnabled && coverArtEnabled;
    baseline.indeterminate = musicBrainzEnabled !== coverArtEnabled;

    for (const control of providerControls) {
      const setting = state.providerSettings.get(control.provider);
      const checkbox = byId(control.enabledId);
      const input = byId(control.keyId);
      checkbox.checked = Boolean(setting && setting.enabled);
      checkbox.dataset.dirty = "false";
      input.value = "";
      input.dataset.dirty = "false";
      input.placeholder = setting && setting.api_key_configured
        ? "API key already configured"
        : "Optional API key";
    }
  }

  /**
   * Persists state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} libraryRoot - Expected to be a value supplied by the caller or server response.
   * @param {unknown} dropboxRoot - Expected to be a value supplied by the caller or server response.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function saveSystemConfig(libraryRoot, dropboxRoot) {
    state.systemConfig = await api("/api/v1/admin/system/config", {
      method: "PUT",
      body: {
        library_root: libraryRoot,
        dropbox_root: dropboxRoot,
      },
    });
  }

  /**
   * Handles configure providers for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * Inputs: None.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function configureProviders() {
    if (!state.setupLoaded) {
      await loadSetupState();
    }

    for (const control of providerControls) {
      const body = providerUpdateBody(control);
      if (!body) {
        continue;
      }
      const setting = await api(`/api/v1/admin/providers/${control.provider}/settings`, {
        method: "PATCH",
        body,
      });
      state.providerSettings.set(setting.provider, setting);
    }
  }

  /**
   * Handles provider update body for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} control - Expected to be a value supplied by the caller or server response.
   * @returns {unknown} Returns the computed value described by the function body.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function providerUpdateBody(control) {
    const setting = state.providerSettings.get(control.provider);
    const enabled = byId(control.enabledId).checked;
    const originalEnabled = setting ? Boolean(setting.enabled) : false;
    const body = {};

    if (!setting || enabled !== originalEnabled) {
      body.enabled = enabled;
    }

    const apiKey = byId(control.keyId).value.trim();
    if (apiKey) {
      body.api_key = apiKey;
    }

    return Object.keys(body).length ? body : null;
  }

  /**
   * Handles start initial scan for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * Inputs: None.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function startInitialScan() {
    await api("/api/v1/admin/maintenance/scans/initial", {
      method: "POST",
      body: {
        reason: "Initial scan started from the first-run admin wizard",
      },
    });

    const status = await loadBootstrapStatus();
    if (!status.initial_scan_started) {
      throw new Error("Initial scan was accepted, but setup completion was not recorded.");
    }
  }

  /**
   * Opens a browser view or route for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} section - Expected to be a value supplied by the caller or server response.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function openDashboardSection(section) {
    state.activeSection = dashboardSections.includes(section) ? section : "dashboard";

    for (const current of dashboardSections) {
      const panel = byId(`section-${current}`);
      if (panel) {
        panel.classList.toggle("hidden", current !== state.activeSection);
      }
    }

    for (const link of document.querySelectorAll(".dashboard nav a")) {
      const selected = link.hash === `#${state.activeSection}`;
      link.classList.toggle("active", selected);
      if (selected) {
        link.setAttribute("aria-current", "page");
      } else {
        link.removeAttribute("aria-current");
      }
    }

    if (state.activeSection === "providers") {
      stopDashboardPolling();
      await loadProviderHealth();
      return;
    }
    if (state.activeSection === "failures") {
      stopDashboardPolling();
      await loadFailures();
      return;
    }
    if (state.activeSection === "users") {
      stopDashboardPolling();
      await loadUsers();
      return;
    }
    await loadDashboardData();
  }

  /**
   * Loads persisted state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * Inputs: None.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function loadDashboardData() {
    setDashboardMessage("Loading dashboard...", "warning");
    setBusy(true);
    setDashboardBusy(true);

    try {
      const [summary, transcodeSlots] = await Promise.all([
        api("/api/v1/admin/maintenance/summary"),
        api("/api/v1/admin/media/transcode-slots"),
      ]);
      state.summary = summary;
      state.transcodeSlots = transcodeSlots;
      renderDashboard();
      if (Number(summary.scanning) > 0) {
        setDashboardMessage("Active scan progress is refreshing automatically.", "success");
      } else {
        setDashboardMessage("Dashboard is up to date.", "success");
      }
    } catch (error) {
      setDashboardMessage(error.message, "error");
    } finally {
      setBusy(false);
      setDashboardBusy(false);
    }
  }

  /**
   * Renders browser UI for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * Inputs: None.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function renderDashboard() {
    const summary = state.summary || {};
    setText("summary-scanning", formatCount(summary.scanning));
    setText("summary-imported", formatCount(summary.imported));
    setText("summary-quarantined", formatCount(summary.quarantined));
    setText("summary-failed", formatCount(summary.failed));
    setText("summary-artists", formatCount(summary.artists));
    setText("summary-albums", formatCount(summary.albums));
    setText("summary-tracks", formatCount(summary.tracks));
    setText("summary-playlists", formatCount(summary.playlists));
    renderScanProgress(summary.active_jobs);

    const slots = state.transcodeSlots || {};
    setText("transcode-in-use", formatCount(slots.in_use));
    setText("transcode-available", formatCount(slots.available));
    setText("transcode-limit", formatCount(slots.limit));
    updateDashboardPolling();
  }

  /**
   * Renders browser UI for active import job progress on the admin dashboard.
   *
   * @param {unknown} activeJobs - Expected to be an array supplied by the dashboard summary response.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function renderScanProgress(activeJobs) {
    const list = byId("scan-progress-list");
    list.replaceChildren();
    const jobs = Array.isArray(activeJobs) ? activeJobs : [];
    if (!jobs.length) {
      const empty = document.createElement("p");
      empty.className = "progress-empty";
      empty.textContent = "No active scans.";
      list.appendChild(empty);
      return;
    }

    for (const job of jobs) {
      const item = document.createElement("article");
      item.className = "progress-item";

      const title = document.createElement("div");
      title.className = "progress-title";
      const name = document.createElement("span");
      name.textContent = formatStatus(job.kind);
      const status = document.createElement("span");
      status.textContent = formatStatus(job.status);
      title.append(name, status);

      const scope = document.createElement("p");
      scope.className = "progress-path";
      scope.textContent = `Scope: ${formatScope(job.scope)}`;

      const metrics = document.createElement("div");
      metrics.className = "progress-metrics";
      for (const metric of [
        { label: "Processed", value: job.processed_files },
        { label: "Published", value: job.published_files },
        { label: "Quarantined", value: job.quarantined_files },
        { label: "Failed", value: job.failed_files, failureJobId: job.id },
      ]) {
        const failedCount = Number(metric.value);
        const hasFailures = metric.failureJobId
          && Number.isFinite(failedCount)
          && failedCount > 0;
        const element = document.createElement(hasFailures ? "a" : "span");
        element.textContent = `${metric.label} ${formatCount(metric.value)}`;
        if (hasFailures) {
          element.href = "#failures";
          element.dataset.action = "view-failures";
          element.dataset.jobId = metric.failureJobId;
        }
        metrics.appendChild(element);
      }

      const time = document.createElement("p");
      time.className = "progress-time";
      const lastProgress = job.last_progress_at || job.updated_at;
      time.textContent = `Started ${formatDate(job.created_at)}. Last progress ${formatDate(lastProgress)}.`;

      item.append(title, scope, metrics, time);
      list.appendChild(item);
    }
  }

  /**
   * Opens import failure diagnostics from an active scan progress link.
   *
   * @param {unknown} event - Expected to be a DOM event object.
   * @returns {void} Returns after updating route and UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function handleScanProgressClick(event) {
    const link = event.target.closest('[data-action="view-failures"]');
    if (!link) {
      return;
    }
    event.preventDefault();
    state.failureFilterJobId = link.dataset.jobId || null;
    window.history.replaceState(null, "", "/admin#failures");
    openDashboardSection("failures");
  }

  /**
   * Loads failed import work items for the admin failure diagnostics view.
   *
   * Inputs: None.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function loadFailures() {
    setFailuresMessage("Loading failures...", "warning");
    setBusy(true);
    setDashboardBusy(true);

    try {
      const query = state.failureFilterJobId
        ? `?import_job_id=${encodeURIComponent(state.failureFilterJobId)}`
        : "";
      const response = await api(`/api/v1/admin/maintenance/failures${query}`);
      state.failures = response && Array.isArray(response.failures)
        ? response.failures
        : [];
      renderFailures();
      setFailuresMessage(
        state.failureFilterJobId
          ? `Showing failures for job ${state.failureFilterJobId}.`
          : "Failures are up to date.",
        state.failures.length ? "warning" : "success"
      );
    } catch (error) {
      setFailuresMessage(error.message, "error");
    } finally {
      setBusy(false);
      setDashboardBusy(false);
    }
  }

  /**
   * Renders failed import work items in the admin failure diagnostics view.
   *
   * Inputs: None.
   * @returns {void} Returns after updating DOM state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function renderFailures() {
    const clearButton = byId("clear-failures-filter-button");
    clearButton.classList.toggle("hidden", !state.failureFilterJobId);

    const table = byId("failures-table-body");
    table.replaceChildren();
    if (!state.failures.length) {
      appendEmptyRow(table, 6, "No failed import items.");
      return;
    }

    for (const failure of state.failures) {
      const row = document.createElement("tr");
      appendCell(row, formatDate(failure.updated_at));
      appendCell(row, formatStatus(failure.import_job_kind));
      appendCell(row, formatCount(failure.attempts));
      appendCell(row, failure.source_path || "-");
      appendCell(row, failure.last_error || "-");
      appendCell(row, failure.import_job_id || "-");
      table.appendChild(row);
    }
  }

  /**
   * Loads persisted state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * Inputs: None.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function loadProviderHealth() {
    setProvidersMessage("Loading providers...", "warning");
    setBusy(true);
    setDashboardBusy(true);

    try {
      const response = await api("/api/v1/admin/providers/health");
      state.providerHealth = response && Array.isArray(response.providers)
        ? response.providers
        : [];
      renderProviderHealth();
      setProvidersMessage("Provider health is up to date.", "success");
    } catch (error) {
      setProvidersMessage(error.message, "error");
    } finally {
      setBusy(false);
      setDashboardBusy(false);
    }
  }

  /**
   * Renders browser UI for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * Inputs: None.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function renderProviderHealth() {
    const table = byId("providers-table-body");
    table.replaceChildren();
    if (!state.providerHealth.length) {
      appendEmptyRow(table, 7, "No providers are configured.");
      return;
    }

    for (const provider of state.providerHealth) {
      const row = document.createElement("tr");
      appendCell(row, provider.display_name || provider.provider || "-");
      appendCell(row, formatStatus(provider.status));
      appendCell(row, provider.enabled ? "Yes" : "No");
      appendCell(row, provider.api_key_configured ? "Configured" : "Not configured");
      appendCell(row, provider.maintenance_ready ? "Ready" : "Waiting");
      appendCell(row, formatCount(provider.failure_count));
      appendCell(row, formatDate(provider.retry_after));
      table.appendChild(row);
    }
  }

  /**
   * Loads persisted state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * Inputs: None.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function loadUsers() {
    setUsersMessage("Loading users...", "warning");
    setBusy(true);
    setDashboardBusy(true);

    try {
      const response = await api("/api/v1/admin/users");
      state.users = response && Array.isArray(response.users) ? response.users : [];
      renderUsers();
      setUsersMessage("Users are up to date.", "success");
    } catch (error) {
      setUsersMessage(error.message, "error");
    } finally {
      setBusy(false);
      setDashboardBusy(false);
    }
  }

  /**
   * Renders browser UI for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * Inputs: None.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function renderUsers() {
    const table = byId("users-table-body");
    table.replaceChildren();
    if (!state.users.length) {
      appendEmptyRow(table, 6, "No users exist.");
      return;
    }

    for (const user of state.users) {
      const row = document.createElement("tr");
      appendCell(row, user.username || "-");
      appendCell(row, formatStatus(user.role));
      appendCell(row, user.disabled ? "Disabled" : "Enabled");
      appendCell(row, formatDate(user.created_at));

      const passwordCell = document.createElement("td");
      const passwordInput = document.createElement("input");
      passwordInput.type = "password";
      passwordInput.autocomplete = "new-password";
      passwordInput.setAttribute("aria-label", `New password for ${user.username}`);
      passwordInput.dataset.passwordInput = user.id;
      passwordCell.appendChild(passwordInput);
      row.appendChild(passwordCell);

      const actionCell = document.createElement("td");
      const resetButton = document.createElement("button");
      resetButton.type = "button";
      resetButton.textContent = "Reset";
      resetButton.dataset.action = "reset-password";
      resetButton.dataset.userId = user.id;
      actionCell.appendChild(resetButton);

      const deleteButton = document.createElement("button");
      deleteButton.type = "button";
      deleteButton.textContent = "Delete";
      deleteButton.dataset.action = "delete-user";
      deleteButton.dataset.userId = user.id;
      actionCell.appendChild(deleteButton);
      row.appendChild(actionCell);

      table.appendChild(row);
    }
  }

  /**
   * Creates a new resource for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} event - Expected to be a DOM event object.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function createUser(event) {
    event.preventDefault();
    if (state.busy) {
      return;
    }

    const username = byId("create-user-username").value.trim();
    const password = byId("create-user-password").value;
    const role = byId("create-user-role").value;
    if (!username || !password) {
      setUsersMessage("Username and password are required.", "warning");
      return;
    }

    setBusy(true);
    setDashboardBusy(true);
    setUsersMessage("Creating user...", "warning");
    try {
      await api("/api/v1/admin/users", {
        method: "POST",
        body: { username, password, role },
      });
      byId("create-user-form").reset();
      await loadUsersAfterMutation("User created.");
    } catch (error) {
      setUsersMessage(error.message, "error");
    } finally {
      setBusy(false);
      setDashboardBusy(false);
    }
  }

  /**
   * Handles handle user table click for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} event - Expected to be a DOM event object.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function handleUserTableClick(event) {
    const button = event.target.closest("button[data-action]");
    if (!button || state.busy) {
      return;
    }

    const userId = button.dataset.userId;
    if (button.dataset.action === "reset-password") {
      await resetUserPassword(userId);
      return;
    }
    if (button.dataset.action === "delete-user") {
      await deleteUser(userId);
    }
  }

  /**
   * Resets stored state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} userId - Expected to be a value supplied by the caller or server response.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function resetUserPassword(userId) {
    const input = document.querySelector(`[data-password-input="${userId}"]`);
    const password = input ? input.value : "";
    if (!password) {
      setUsersMessage("Enter a new password before resetting.", "warning");
      return;
    }

    setBusy(true);
    setDashboardBusy(true);
    setUsersMessage("Resetting password...", "warning");
    try {
      await api(`/api/v1/admin/users/${userId}/password-reset`, {
        method: "POST",
        body: { password },
      });
      if (input) {
        input.value = "";
      }
      await loadUsersAfterMutation("Password reset.");
    } catch (error) {
      setUsersMessage(error.message, "error");
    } finally {
      setBusy(false);
      setDashboardBusy(false);
    }
  }

  /**
   * Deletes or removes a resource from browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} userId - Expected to be a value supplied by the caller or server response.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function deleteUser(userId) {
    const user = state.users.find((candidate) => candidate.id === userId);
    const username = user && user.username ? user.username : "this user";
    if (!window.confirm(`Delete ${username}?`)) {
      return;
    }

    setBusy(true);
    setDashboardBusy(true);
    setUsersMessage("Deleting user...", "warning");
    try {
      await api(`/api/v1/admin/users/${userId}`, { method: "DELETE" });
      await loadUsersAfterMutation("User deleted.");
    } catch (error) {
      setUsersMessage(error.message, "error");
    } finally {
      setBusy(false);
      setDashboardBusy(false);
    }
  }

  /**
   * Loads persisted state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} message - Expected to be a value supplied by the caller or server response.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function loadUsersAfterMutation(message) {
    const response = await api("/api/v1/admin/users");
    state.users = response && Array.isArray(response.users) ? response.users : [];
    renderUsers();
    setUsersMessage(message, "success");
  }

  /**
   * Starts an asynchronous operation for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * Inputs: None.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function triggerFullRescan() {
    if (state.busy) {
      return;
    }
    await triggerRescan("/api/v1/admin/maintenance/rescans/full", {
      reason: "Full rescan requested from the admin dashboard",
    });
  }

  /**
   * Starts an asynchronous operation for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} event - Expected to be a DOM event object.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function triggerSubtreeRescan(event) {
    event.preventDefault();
    if (state.busy) {
      return;
    }
    const path = byId("subtree-rescan-path").value.trim();
    if (!path) {
      setDashboardMessage("Enter a subtree path before starting a rescan.", "warning");
      return;
    }
    await triggerRescan("/api/v1/admin/maintenance/rescans/subtree", {
      path,
      reason: "Subtree rescan requested from the admin dashboard",
    });
  }

  /**
   * Starts an asynchronous operation for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} path - Expected to be a value supplied by the caller or server response.
   * @param {unknown} body - Expected to be a value supplied by the caller or server response.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function triggerRescan(path, body) {
    setBusy(true);
    setDashboardBusy(true);
    setDashboardMessage("Starting rescan...", "warning");
    try {
      const response = await api(path, { method: "POST", body });
      const reused = response && response.reused_existing ? " Existing job reused." : "";
      setDashboardMessage(`Rescan accepted.${reused}`, "success");
      await refreshDashboardSilently();
    } catch (error) {
      setDashboardMessage(error.message, "error");
    } finally {
      setBusy(false);
      setDashboardBusy(false);
    }
  }

  /**
   * Refreshes cached state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * Inputs: None.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function refreshDashboardSilently() {
    const [summary, transcodeSlots] = await Promise.all([
      api("/api/v1/admin/maintenance/summary"),
      api("/api/v1/admin/media/transcode-slots"),
    ]);
    state.summary = summary;
    state.transcodeSlots = transcodeSlots;
    renderDashboard();
  }

  /**
   * Starts or stops dashboard polling based on active scan state.
   *
   * Inputs: None.
   * @returns {void} Returns after updating local polling state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function updateDashboardPolling() {
    const dashboardVisible = !byId("dashboard-view").classList.contains("hidden");
    const scanning = Number(state.summary && state.summary.scanning);
    const shouldPoll = dashboardVisible
      && state.activeSection === "dashboard"
      && Number.isFinite(scanning)
      && scanning > 0;

    if (shouldPoll) {
      startDashboardPolling();
    } else {
      stopDashboardPolling();
    }
  }

  /**
   * Starts dashboard polling for active import progress.
   *
   * Inputs: None.
   * @returns {void} Returns after updating local polling state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function startDashboardPolling() {
    if (state.dashboardPollId !== null) {
      return;
    }
    state.dashboardPollId = window.setInterval(pollDashboardData, 5000);
  }

  /**
   * Stops dashboard polling for active import progress.
   *
   * Inputs: None.
   * @returns {void} Returns after updating local polling state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function stopDashboardPolling() {
    if (state.dashboardPollId === null) {
      return;
    }
    window.clearInterval(state.dashboardPollId);
    state.dashboardPollId = null;
  }

  /**
   * Refreshes active dashboard data in the background while scans are running.
   *
   * Inputs: None.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function pollDashboardData() {
    if (state.dashboardPolling || state.busy) {
      return;
    }
    state.dashboardPolling = true;
    try {
      await refreshDashboardSilently();
    } catch (error) {
      setDashboardMessage(error.message, "error");
      stopDashboardPolling();
    } finally {
      state.dashboardPolling = false;
    }
  }

  /**
   * Creates a new resource for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} username - Expected to be a value supplied by the caller or server response.
   * @param {unknown} password - Expected to be a value supplied by the caller or server response.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function createFirstAdminOrUseExisting(username, password) {
    try {
      state.account = await api("/api/v1/bootstrap/first-admin", {
        method: "POST",
        auth: false,
        body: { username, password },
      });
      return;
    } catch (error) {
      if (error.status !== 409) {
        throw error;
      }
    }

    const account = await api("/api/v1/auth/me");
    if (account.role !== "admin") {
      throw new Error("The existing account is not an admin.");
    }
    state.account = account;
  }

  /**
   * Opens a browser view or route for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * Inputs: None.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function openFirstAdminSetup() {
    state.setupMode = "first-admin";
    state.setupLoaded = false;
    state.systemConfig = null;
    state.providerSettings = new Map();
    showOnly("setup-view");
    setSetupState("First run");
    setSetupMessage("", "hidden");
    clearSteps();
    markStep("admin", "active");
    applyWizardMode();
  }

  /**
   * Opens a browser view or route for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} message - Expected to be a value supplied by the caller or server response.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function openResumableSetup(message) {
    state.setupMode = "resume";
    showOnly("setup-view");
    setSetupState("Setup pending");
    setLoginMessage("", "hidden");
    clearSteps();
    markStep("admin", "complete");
    markStep("paths", "active");
    setSetupMessage(message, "success");
    applyWizardMode();
  }

  /**
   * Runs the operation for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} step - Expected to be a value supplied by the caller or server response.
   * @param {unknown} message - Expected to be a value supplied by the caller or server response.
   * @param {unknown} task - Expected to be a value supplied by the caller or server response.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function runStep(step, message, task) {
    markStep(step, "active");
    setSetupMessage(message, "warning");
    await task();
    markStep(step, "complete");
  }

  /**
   * Calls the Harmonixia JSON API from the admin UI.
   *
   * @param {unknown} path - Expected to be a value supplied by the caller or server response.
   * @param {unknown} options - Expected to be a value supplied by the caller or server response.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function api(path, options) {
    const settings = options || {};
    const headers = {
      Accept: "application/json",
    };

    if (settings.body !== undefined) {
      headers["Content-Type"] = "application/json";
    }

    if (settings.auth !== false) {
      if (!state.credentials) {
        throw new Error("Admin credentials are required.");
      }
      headers.Authorization = basicAuth(state.credentials);
    }

    const response = await fetch(path, {
      method: settings.method || "GET",
      headers,
      body: settings.body === undefined ? undefined : JSON.stringify(settings.body),
    });

    const text = await response.text();
    const data = text ? parseJson(text) : null;

    if (!response.ok) {
      const detail = data && data.message ? data.message : `${response.status} ${response.statusText}`;
      const error = new Error(detail);
      error.status = response.status;
      throw error;
    }

    return data;
  }

  /**
   * Parses and validates input for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} text - Expected to be a value supplied by the caller or server response.
   * @returns {unknown} Returns the computed value described by the function body.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function parseJson(text) {
    try {
      return JSON.parse(text);
    } catch (_error) {
      return null;
    }
  }

  /**
   * Handles basic auth for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} credentials - Expected to be a value supplied by the caller or server response.
   * @returns {unknown} Returns the computed value described by the function body.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function basicAuth(credentials) {
    const bytes = new TextEncoder().encode(`${credentials.username}:${credentials.password}`);
    let binary = "";
    for (const byte of bytes) {
      binary += String.fromCharCode(byte);
    }
    return `Basic ${window.btoa(binary)}`;
  }

  /**
   * Opens a browser view or route for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * Inputs: None.
   * @returns {Promise<unknown>} Resolves when the async UI/API operation completes.
   * @throws {Error} Throws when required DOM state is missing, validation fails, or an API request rejects.
   */
  async function openDashboard() {
    showOnly("dashboard-view");
    setLoginMessage("", "hidden");
    await openDashboardSection(sectionFromHash());
  }

  /**
   * Handles section from hash for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * Inputs: None.
   * @returns {unknown} Returns the computed value described by the function body.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function sectionFromHash() {
    const section = window.location.hash.replace("#", "");
    return dashboardSections.includes(section) ? section : "dashboard";
  }

  /**
   * Handles show only for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} viewId - Expected to be a value supplied by the caller or server response.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function showOnly(viewId) {
    for (const id of ["setup-view", "login-view", "dashboard-view"]) {
      byId(id).classList.toggle("hidden", id !== viewId);
    }
    if (viewId !== "dashboard-view") {
      stopDashboardPolling();
    }
  }

  /**
   * Sets stored state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} message - Expected to be a value supplied by the caller or server response.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function setSetupState(message) {
    byId("setup-state").textContent = message;
  }

  /**
   * Sets stored state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} message - Expected to be a value supplied by the caller or server response.
   * @param {unknown} kind - Expected to be a value supplied by the caller or server response.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function setSetupMessage(message, kind) {
    setMessage(byId("setup-message"), message, kind);
  }

  /**
   * Sets stored state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} message - Expected to be a value supplied by the caller or server response.
   * @param {unknown} kind - Expected to be a value supplied by the caller or server response.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function setLoginMessage(message, kind) {
    setMessage(byId("login-message"), message, kind);
  }

  /**
   * Sets stored state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} message - Expected to be a value supplied by the caller or server response.
   * @param {unknown} kind - Expected to be a value supplied by the caller or server response.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function setDashboardMessage(message, kind) {
    setMessage(byId("dashboard-message"), message, kind);
  }

  /**
   * Sets stored state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} message - Expected to be a value supplied by the caller or server response.
   * @param {unknown} kind - Expected to be a value supplied by the caller or server response.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function setProvidersMessage(message, kind) {
    setMessage(byId("providers-message"), message, kind);
  }

  /**
   * Sets stored state for browser admin UI failure diagnostics messaging.
   *
   * @param {unknown} message - Expected to be a value supplied by the caller or server response.
   * @param {unknown} kind - Expected to be a value supplied by the caller or server response.
   * @returns {void} Returns after updating DOM state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function setFailuresMessage(message, kind) {
    setMessage(byId("failures-message"), message, kind);
  }

  /**
   * Sets stored state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} message - Expected to be a value supplied by the caller or server response.
   * @param {unknown} kind - Expected to be a value supplied by the caller or server response.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function setUsersMessage(message, kind) {
    setMessage(byId("users-message"), message, kind);
  }

  /**
   * Sets stored state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} element - Expected to be a value supplied by the caller or server response.
   * @param {unknown} message - Expected to be a value supplied by the caller or server response.
   * @param {unknown} kind - Expected to be a value supplied by the caller or server response.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function setMessage(element, message, kind) {
    element.textContent = message;
    element.className = "notice";
    if (!message || kind === "hidden") {
      element.classList.add("hidden");
      return;
    }
    element.classList.add(kind);
  }

  /**
   * Sets stored state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} id - Expected to be a value supplied by the caller or server response.
   * @param {unknown} value - Expected to be a value supplied by the caller or server response.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function setText(id, value) {
    byId(id).textContent = value;
  }

  /**
   * Handles append cell for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} row - Expected to be a value supplied by the caller or server response.
   * @param {unknown} value - Expected to be a value supplied by the caller or server response.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function appendCell(row, value) {
    const cell = document.createElement("td");
    cell.textContent = value;
    row.appendChild(cell);
  }

  /**
   * Handles append empty row for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} table - Expected to be a value supplied by the caller or server response.
   * @param {unknown} colspan - Expected to be a value supplied by the caller or server response.
   * @param {unknown} message - Expected to be a value supplied by the caller or server response.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function appendEmptyRow(table, colspan, message) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = colspan;
    cell.textContent = message;
    row.appendChild(cell);
    table.appendChild(row);
  }

  /**
   * Formats display data for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} value - Expected to be a value supplied by the caller or server response.
   * @returns {unknown} Returns the computed value described by the function body.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function formatCount(value) {
    const number = Number(value);
    return Number.isFinite(number) ? number.toLocaleString() : "-";
  }

  /**
   * Formats display data for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} value - Expected to be a value supplied by the caller or server response.
   * @returns {unknown} Returns the computed value described by the function body.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function formatStatus(value) {
    if (!value) {
      return "-";
    }
    return String(value)
      .replace(/_/g, " ")
      .replace(/\b\w/g, (letter) => letter.toUpperCase());
  }

  /**
   * Formats display data for a maintenance scope.
   *
   * @param {unknown} scope - Expected to be a maintenance scope from the dashboard summary response.
   * @returns {unknown} Returns display text for the scope.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function formatScope(scope) {
    if (!scope || typeof scope !== "object") {
      return "Full library";
    }
    if (scope.type === "path" && scope.path) {
      return String(scope.path);
    }
    if (scope.type) {
      return formatStatus(scope.type);
    }
    return "Full library";
  }

  /**
   * Formats display data for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} value - Expected to be a value supplied by the caller or server response.
   * @returns {unknown} Returns the computed value described by the function body.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function formatDate(value) {
    if (!value) {
      return "-";
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
      return String(value);
    }
    return date.toLocaleString();
  }

  /**
   * Clears stored state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * Inputs: None.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function clearSteps() {
    for (const element of document.querySelectorAll(".step")) {
      element.classList.remove("active", "complete", "error");
    }
  }

  /**
   * Marks UI or workflow state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} step - Expected to be a value supplied by the caller or server response.
   * @param {unknown} status - Expected to be a value supplied by the caller or server response.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function markStep(step, status) {
    for (const element of document.querySelectorAll(".step")) {
      if (status === "active") {
        element.classList.remove("active");
      }
    }

    const element = document.querySelector(`.step[data-step="${step}"]`);
    if (!element) {
      return;
    }
    element.classList.remove("active", "complete", "error");
    element.classList.add(status);
  }

  /**
   * Marks UI or workflow state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * Inputs: None.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function markActiveStepError() {
    const active = document.querySelector(".step.active");
    if (active) {
      active.classList.remove("active");
      active.classList.add("error");
    }
  }

  /**
   * Sets stored state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} disabled - Expected to be a value supplied by the caller or server response.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function setWizardBusy(disabled) {
    byId("start-scan-button").disabled = disabled;
    byId("back-button").disabled = disabled;
    if (disabled) {
      for (const input of byId("setup-form").querySelectorAll("input")) {
        if (input.id !== "provider-musicbrainz") {
          input.disabled = true;
        }
      }
      return;
    }
    applyWizardMode();
  }

  /**
   * Applies derived state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * Inputs: None.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function applyWizardMode() {
    const creatingFirstAdmin = state.setupMode === "first-admin";
    const adminGroup = document.querySelector('[data-group="admin"]');
    adminGroup.classList.toggle("hidden", !creatingFirstAdmin);
    setGroupHidden("paths", creatingFirstAdmin);
    setGroupHidden("providers", creatingFirstAdmin);
    setGroupHidden("scan", creatingFirstAdmin);
    setGroupDisabled("admin", !creatingFirstAdmin);
    setGroupDisabled("paths", creatingFirstAdmin);
    setGroupDisabled("providers", creatingFirstAdmin);
    setGroupDisabled("scan", creatingFirstAdmin);
    byId("admin-user").required = creatingFirstAdmin;
    byId("admin-pass").required = creatingFirstAdmin;
    byId("library-root").required = !creatingFirstAdmin;
    byId("dropbox-root").required = !creatingFirstAdmin;
    byId("provider-musicbrainz").disabled = true;
    byId("start-scan-button").textContent = creatingFirstAdmin
      ? "Create Admin and Load Settings"
      : "Save and Start Initial Scan";
  }

  /**
   * Sets stored state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} group - Expected to be a value supplied by the caller or server response.
   * @param {unknown} hidden - Expected to be a value supplied by the caller or server response.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function setGroupHidden(group, hidden) {
    const element = document.querySelector(`[data-group="${group}"]`);
    if (element) {
      element.classList.toggle("hidden", hidden);
    }
  }

  /**
   * Sets stored state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} group - Expected to be a value supplied by the caller or server response.
   * @param {unknown} disabled - Expected to be a value supplied by the caller or server response.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function setGroupDisabled(group, disabled) {
    const element = document.querySelector(`[data-group="${group}"]`);
    if (!element) {
      return;
    }
    for (const input of element.querySelectorAll("input")) {
      if (input.id !== "provider-musicbrainz") {
        input.disabled = disabled;
      }
    }
  }

  /**
   * Sets stored state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} busy - Expected to be a value supplied by the caller or server response.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function setBusy(busy) {
    state.busy = busy;
    const loginButton = byId("login-button");
    if (loginButton) {
      loginButton.disabled = busy;
    }
  }

  /**
   * Sets stored state for browser admin UI state, forms, API requests, and dashboard rendering.
   *
   * @param {unknown} disabled - Expected to be a value supplied by the caller or server response.
   * @returns {void} Returns after updating DOM state or local UI state.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function setDashboardBusy(disabled) {
    for (const button of byId("dashboard-view").querySelectorAll("button")) {
      button.disabled = disabled;
    }
  }

  /**
   * Finds a required DOM element by id.
   *
   * @param {unknown} id - Expected to be a value supplied by the caller or server response.
   * @returns {unknown} Returns the computed value described by the function body.
   * @throws {Error} Does not intentionally throw recoverable errors.
   */
  function byId(id) {
    return document.getElementById(id);
  }
})();
