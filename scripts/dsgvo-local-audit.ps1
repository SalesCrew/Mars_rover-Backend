param(
  [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'

$backendRoot = Resolve-Path (Join-Path $PSScriptRoot '..')
$frontendRoot = Resolve-Path (Join-Path $backendRoot '..')
$expectedMetadataVerifierCheckCount = 28

function Assert-NoMatches {
  param(
    [string]$Label,
    [string[]]$Path,
    [string[]]$Patterns
  )

  $matches = Select-String -Path $Path -Pattern $Patterns -ErrorAction SilentlyContinue
  if ($matches) {
    Write-Host "FAIL $Label"
    $matches | ForEach-Object { Write-Host ("  " + $_.Path + ":" + $_.LineNumber + " " + $_.Line.Trim()) }
    throw "$Label failed"
  }

  Write-Host "PASS $Label"
}

function Assert-PatternOrder {
  param(
    [string]$Label,
    [string]$Path,
    [string]$Before,
    [string]$After
  )

  $beforeMatch = Select-String -Path $Path -Pattern $Before | Select-Object -First 1
  $afterMatch = Select-String -Path $Path -Pattern $After | Select-Object -First 1

  if (!$beforeMatch -or !$afterMatch -or $beforeMatch.LineNumber -ge $afterMatch.LineNumber) {
    Write-Host "FAIL $Label"
    throw "$Label failed"
  }

  Write-Host "PASS $Label"
}

function Assert-FileContainsPatterns {
  param(
    [string]$Label,
    [string]$Path,
    [string[]]$Patterns
  )

  $text = Get-Content -Raw $Path
  $missing = @()
  foreach ($pattern in $Patterns) {
    if ($text -notmatch $pattern) {
      $missing += $pattern
    }
  }

  if ($missing.Count -gt 0) {
    Write-Host "FAIL $Label"
    $missing | ForEach-Object { Write-Host ("  missing pattern: " + $_) }
    throw "$Label failed"
  }

  Write-Host "PASS $Label"
}

function Assert-NoRawRegexMatches {
  param(
    [string]$Label,
    [string]$Path,
    [string[]]$Patterns
  )

  $text = Get-Content -Raw $Path
  $violations = @()
  foreach ($pattern in $Patterns) {
    $matches = [regex]::Matches($text, $pattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    foreach ($match in $matches) {
      $lineNumber = ($text.Substring(0, $match.Index) -split "`r?`n").Count
      $snippet = ($match.Value -replace '\s+', ' ').Trim()
      $violations += "${Path}:$lineNumber $snippet"
    }
  }

  if ($violations.Count -gt 0) {
    Write-Host "FAIL $Label"
    $violations | ForEach-Object { Write-Host ("  " + $_) }
    throw "$Label failed"
  }

  Write-Host "PASS $Label"
}

function Assert-NoTrailingWhitespaceInFiles {
  param(
    [string]$Label,
    [string[]]$Path
  )

  $violations = @()
  foreach ($pathItem in $Path) {
    $files = Get-ChildItem -Path $pathItem -Recurse -File -ErrorAction SilentlyContinue
    foreach ($file in $files) {
      $lineNumber = 0
      foreach ($line in Get-Content $file.FullName) {
        $lineNumber++
        if ($line -match '\s+$') {
          $violations += "$($file.FullName):$lineNumber trailing whitespace"
        }
      }
    }
  }

  if ($violations.Count -gt 0) {
    Write-Host "FAIL $Label"
    $violations | Select-Object -First 50 | ForEach-Object { Write-Host ("  " + $_) }
    if ($violations.Count -gt 50) {
      Write-Host ("  ... and " + ($violations.Count - 50) + " more")
    }
    throw "$Label failed"
  }

  Write-Host "PASS $Label"
}

function Assert-NoMojibakeInFiles {
  param(
    [string]$Label,
    [string[]]$Path
  )

  $c3 = [char]0x00C3
  $c2 = [char]0x00C2
  $c6 = [char]0x00C6
  $latinSmallFHook = [char]0x0192
  $eth = [char]0x00F0
  $yDiaeresis = [char]0x0178
  $iDiaeresis = [char]0x00EF
  $replacementChar = [char]0xFFFD
  $patterns = @(
    [regex]::Escape("$c3"),
    [regex]::Escape("$c2"),
    [regex]::Escape("$c6"),
    [regex]::Escape("$latinSmallFHook"),
    [regex]::Escape("$eth$yDiaeresis"),
    [regex]::Escape("$iDiaeresis"),
    [regex]::Escape("$replacementChar")
  )
  $matches = Select-String -Path $Path -Pattern $patterns -ErrorAction SilentlyContinue
  if ($matches) {
    Write-Host "FAIL $Label"
    $matches | Select-Object -First 80 | ForEach-Object {
      Write-Host ("  " + $_.Path + ":" + $_.LineNumber + " " + $_.Line.Trim())
    }
    if (($matches | Measure-Object).Count -gt 80) {
      Write-Host ("  ... and " + (($matches | Measure-Object).Count - 80) + " more")
    }
    throw "$Label failed"
  }

  Write-Host "PASS $Label"
}

function Assert-HardeningSqlTransactionGuardrailsOrdered {
  param(
    [string]$Path
  )

  $text = Get-Content -Raw $Path
  $requiredSequence = @(
    @{ Name = 'begin'; Pattern = '(?m)^begin;$' },
    @{ Name = 'lock_timeout'; Pattern = "(?m)^set local lock_timeout = '5s';$" },
    @{ Name = 'statement_timeout'; Pattern = "(?m)^set local statement_timeout = '5min';$" },
    @{ Name = 'first_schema_change'; Pattern = '(?m)^create schema if not exists app_private;$' },
    @{ Name = 'commit'; Pattern = '(?m)^commit;$' }
  )

  $positions = @{}
  foreach ($item in $requiredSequence) {
    $matches = [regex]::Matches($text, $item.Pattern)
    if ($matches.Count -ne 1) {
      Write-Host 'FAIL RLS hardening SQL transaction guardrails are ordered'
      Write-Host "  $($item.Name) expected exactly once, found $($matches.Count)"
      throw 'RLS hardening SQL transaction guardrail count failed'
    }
    $positions[$item.Name] = $matches[0].Index
  }

  for ($i = 1; $i -lt $requiredSequence.Count; $i++) {
    $previous = $requiredSequence[$i - 1].Name
    $current = $requiredSequence[$i].Name
    if ($positions[$previous] -ge $positions[$current]) {
      Write-Host 'FAIL RLS hardening SQL transaction guardrails are ordered'
      Write-Host "  $previous must appear before $current"
      throw 'RLS hardening SQL transaction guardrail order failed'
    }
  }

  Write-Host 'PASS RLS hardening SQL transaction guardrails are ordered'
}

function Assert-MetadataSqlReadOnlyTransactionGuardrailsOrdered {
  param(
    [string]$Label,
    [string]$Path
  )

  $text = Get-Content -Raw $Path
  $requiredSequence = @(
    @{ Name = 'begin'; Pattern = '(?m)^begin;$' },
    @{ Name = 'read_only'; Pattern = '(?m)^set transaction read only;$' },
    @{ Name = 'lock_timeout'; Pattern = "(?m)^set local lock_timeout = '2s';$" },
    @{ Name = 'statement_timeout'; Pattern = "(?m)^set local statement_timeout = '2min';$" },
    @{ Name = 'commit'; Pattern = '(?m)^commit;$' }
  )

  $positions = @{}
  foreach ($item in $requiredSequence) {
    $matches = [regex]::Matches($text, $item.Pattern)
    if ($matches.Count -ne 1) {
      Write-Host "FAIL $Label"
      Write-Host "  $($item.Name) expected exactly once, found $($matches.Count)"
      throw "$Label failed"
    }
    $positions[$item.Name] = $matches[0].Index
  }

  for ($i = 1; $i -lt $requiredSequence.Count; $i++) {
    $previous = $requiredSequence[$i - 1].Name
    $current = $requiredSequence[$i].Name
    if ($positions[$previous] -ge $positions[$current]) {
      Write-Host "FAIL $Label"
      Write-Host "  $previous must appear before $current"
      throw "$Label failed"
    }
  }

  Write-Host "PASS $Label"
}

function Assert-MetadataSqlAvoidsBusinessDataSources {
  param(
    [string]$Label,
    [string]$Path
  )

  $appTables = @(
    'users',
    'gebietsleiter',
    'gl_onboarding_reads',
    'markets',
    'products',
    'products_update',
    'action_history',
    'bug_reports',
    'fb_questions',
    'fb_modules',
    'fb_module_questions',
    'fb_module_rules',
    'fb_fragebogen',
    'fb_fragebogen_modules',
    'fb_fragebogen_markets',
    'fb_responses',
    'fb_response_answers',
    'fb_zeiterfassung_submissions',
    'fb_zusatz_zeiterfassung',
    'fb_day_tracking',
    'zeiterfassung_wochen_checks',
    'wellen',
    'wellen_displays',
    'wellen_kartonware',
    'wellen_einzelprodukte',
    'wellen_kw_days',
    'wellen_markets',
    'wellen_paletten',
    'wellen_paletten_products',
    'wellen_schuetten',
    'wellen_schuetten_products',
    'wellen_gl_progress',
    'wellen_submissions',
    'wellen_photos',
    'wellen_photo_tags',
    'vorverkauf_entries',
    'vorverkauf_items',
    'vorverkauf_wellen',
    'vorverkauf_wellen_markets',
    'vorverkauf_submissions',
    'vorverkauf_submission_products',
    'nara_incentive_submissions',
    'nara_incentive_items',
    'market_visits'
  )

  $text = Get-Content -Raw $Path
  $violations = @()

  foreach ($tableName in $appTables) {
    $escapedTable = [regex]::Escape($tableName)
    $pattern = "(?im)\b(from|join)\s+(public\.)?$escapedTable\b"
    foreach ($match in [regex]::Matches($text, $pattern)) {
      $lineNumber = ($text.Substring(0, $match.Index) -split "`r?`n").Count
      $violations += "${Path}:$lineNumber $($match.Value.Trim())"
    }
  }

  foreach ($match in [regex]::Matches($text, '(?im)\b(from|join)\s+storage\.objects\b')) {
    $lineNumber = ($text.Substring(0, $match.Index) -split "`r?`n").Count
    $violations += "${Path}:$lineNumber $($match.Value.Trim())"
  }

  if ($violations.Count -gt 0) {
    Write-Host "FAIL $Label"
    $violations | ForEach-Object { Write-Host ("  " + $_) }
    throw "$Label failed"
  }

  Write-Host "PASS $Label"
}

function Assert-MetadataSqlUsesReviewedSourcesOnly {
  param(
    [string]$Label,
    [string]$Path
  )

  $reviewedSources = @(
    'aclexplode',
    'app_tables',
    'default_acl_targets',
    'effective_default_acl',
    'expected_policies',
    'expected_policy_shapes',
    'expected_reviewed_storage_buckets',
    'expected_storage_policies',
    'information_schema.routine_privileges',
    'information_schema.routines',
    'information_schema.table_privileges',
    'information_schema.usage_privileges',
    'lateral',
    'missing_service_role_sequence_access',
    'missing_service_role_table_access',
    'pg_class',
    'pg_default_acl',
    'pg_depend',
    'pg_extension',
    'pg_matviews',
    'pg_namespace',
    'pg_policies',
    'pg_proc',
    'pg_roles',
    'pg_tables',
    'pg_views',
    'policy_shape_violations',
    'policy_shapes',
    'postgres_owner',
    'protected_functions',
    'protected_views',
    'public_application_functions',
    'public_sequence_privileges',
    'public_sequences',
    'public_views',
    'required_private_helper_execute',
    'required_private_helper_schema_usage',
    'required_public_schema_usage',
    'required_service_role_privileges',
    'required_service_role_sequence_privileges',
    'reviewed_public_application_functions',
    'reviewed_public_tables',
    'reviewed_public_views',
    'storage.buckets',
    'storage_policy',
    'storage_policy_violations',
    'unnest',
    'violations'
  )

  $text = Get-Content -Raw $Path
  $violations = @()

  foreach ($match in [regex]::Matches($text, '(?im)\b(from|join)\s+([a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)?)')) {
    $sourceName = $match.Groups[2].Value.ToLowerInvariant()
    if ($reviewedSources -notcontains $sourceName) {
      $lineNumber = ($text.Substring(0, $match.Index) -split "`r?`n").Count
      $violations += "${Path}:$lineNumber unreviewed metadata source: $sourceName"
    }
  }

  if ($violations.Count -gt 0) {
    Write-Host "FAIL $Label"
    $violations | ForEach-Object { Write-Host ("  " + $_) }
    throw "$Label failed"
  }

  Write-Host "PASS $Label"
}

function Assert-AllBusinessApiRoutersAfterAuth {
  param(
    [string]$Path
  )

  $authMiddlewareLine = Select-String -Path $Path -Pattern "app\.use\('/api', authenticateToken\)" | Select-Object -First 1
  if (!$authMiddlewareLine) {
    Write-Host 'FAIL global /api auth middleware exists'
    throw 'global /api auth middleware missing'
  }

  $apiRouterMounts = Select-String -Path $Path -Pattern "app\.use\('/api/"
  $violations = $apiRouterMounts | Where-Object {
    $_.Line -notmatch "app\.use\('/api/auth'" -and $_.LineNumber -lt $authMiddlewareLine.LineNumber
  }

  if ($violations) {
    Write-Host 'FAIL all business /api routers are mounted after global auth middleware'
    $violations | ForEach-Object { Write-Host ("  " + $_.Path + ":" + $_.LineNumber + " " + $_.Line.Trim()) }
    throw 'business /api router mounted before auth middleware'
  }

  Write-Host 'PASS all business /api routers are mounted after global auth middleware'
}

function Assert-RouteModulesMountedAndReviewed {
  param(
    [string]$IndexPath,
    [string]$RoutesRoot
  )

  $reviewedRoutes = @(
    @{ File = 'actionHistory.ts'; Import = 'actionHistoryRouter'; Mount = '/api/action-history' },
    @{ File = 'activities.ts'; Import = 'activitiesRouter'; Mount = '/api/activities' },
    @{ File = 'auth.ts'; Import = 'authRouter'; Mount = '/api/auth' },
    @{ File = 'bugReports.ts'; Import = 'bugReportsRouter'; Mount = '/api/bug-reports' },
    @{ File = 'chat.ts'; Import = 'chatRouter'; Mount = '/api/chat' },
    @{ File = 'export.ts'; Import = 'exportRouter'; Mount = '/api/export' },
    @{ File = 'fragebogen.ts'; Import = 'fragebogenRouter'; Mount = '/api/fragebogen' },
    @{ File = 'gebietsleiter.ts'; Import = 'gebietsleiterRouter'; Mount = '/api/gebietsleiter' },
    @{ File = 'maps.ts'; Import = 'mapsRouter'; Mount = '/api/maps' },
    @{ File = 'markets.ts'; Import = 'marketsRouter'; Mount = '/api/markets' },
    @{ File = 'naraIncentive.ts'; Import = 'naraIncentiveRouter'; Mount = '/api/nara-incentive' },
    @{ File = 'products.ts'; Import = 'productsRouter'; Mount = '/api/products' },
    @{ File = 'productsUpdate.ts'; Import = 'productsUpdateRouter'; Mount = '/api/products-update' },
    @{ File = 'vorverkauf.ts'; Import = 'vorverkaufRouter'; Mount = '/api/vorverkauf' },
    @{ File = 'vorverkaufWellen.ts'; Import = 'vorverkaufWellenRouter'; Mount = '/api/vorverkauf-wellen' },
    @{ File = 'wellen.ts'; Import = 'wellenRouter'; Mount = '/api/wellen' },
    @{ File = 'wochenCheck.ts'; Import = 'wochenCheckRouter'; Mount = '/api/wochen-check' }
  )

  $indexText = Get-Content -Raw $IndexPath
  $actualRouteFiles = Get-ChildItem -Path $RoutesRoot -File -Filter '*.ts' | Select-Object -ExpandProperty Name | Sort-Object
  $reviewedRouteFiles = $reviewedRoutes | ForEach-Object { $_.File } | Sort-Object
  $violations = @()

  foreach ($fileName in $actualRouteFiles) {
    if ($reviewedRouteFiles -notcontains $fileName) {
      $violations += "route module is not in the reviewed mount list: $fileName"
    }
  }

  foreach ($fileName in $reviewedRouteFiles) {
    if ($actualRouteFiles -notcontains $fileName) {
      $violations += "reviewed route module is missing from src/routes: $fileName"
    }
  }

  foreach ($route in $reviewedRoutes) {
    $moduleName = [System.IO.Path]::GetFileNameWithoutExtension($route.File)
    $importPattern = "import\s+$($route.Import)\s+from\s+'\.\/routes\/$moduleName'"
    $mountPattern = "app\.use\('$([regex]::Escape($route.Mount))'(?:,\s*requireAdmin)?,\s*$($route.Import)\)"

    if ($indexText -notmatch $importPattern) {
      $violations += "reviewed route module is not imported with expected router variable: $($route.File)"
    }

    if ($indexText -notmatch $mountPattern) {
      $violations += "reviewed route module is not mounted at expected API prefix: $($route.File) -> $($route.Mount)"
    }
  }

  if ($violations.Count -gt 0) {
    Write-Host 'FAIL route modules are mounted and reviewed'
    $violations | ForEach-Object { Write-Host ("  " + $_) }
    throw 'route module mount review failed'
  }

  Write-Host 'PASS route modules are mounted and reviewed'
}

function Assert-AdminRoutersMountedWithRequireAdmin {
  param(
    [string]$Path
  )

  $text = Get-Content -Raw $Path
  $requiredMounts = @(
    "app\.use\('/api/action-history',\s*requireAdmin,\s*actionHistoryRouter\)",
    "app\.use\('/api/activities',\s*requireAdmin,\s*activitiesRouter\)",
    "app\.use\('/api/export',\s*requireAdmin,\s*exportRouter\)",
    "app\.use\('/api/products-update',\s*requireAdmin,\s*productsUpdateRouter\)"
  )

  $missing = @()
  foreach ($mountPattern in $requiredMounts) {
    if ($text -notmatch $mountPattern) {
      $missing += $mountPattern
    }
  }

  if ($missing.Count -gt 0) {
    Write-Host 'FAIL admin-only routers are mounted with requireAdmin'
    $missing | ForEach-Object { Write-Host ("  missing pattern: " + $_) }
    throw 'admin-only router mount guard failed'
  }

  Write-Host 'PASS admin-only routers are mounted with requireAdmin'
}

function Assert-GlPersonalDataRoutesAreScoped {
  param(
    [string]$RoutesRoot
  )

  Assert-FileContainsPatterns `
    -Label 'GL personal master data routes are admin or self scoped' `
    -Path (Join-Path $RoutesRoot 'gebietsleiter.ts') `
    -Patterns @(
      "router\.get\('/',\s*requireAdmin",
      "router\.get\('/:id',\s*requireSelfOrAdmin",
      "router\.post\('/upload-profile-picture',\s*requireAdmin",
      "router\.post\('/:id/change-password',\s*requireSelfOrAdmin",
      "select\('id, name, address, postal_code, city, phone, email, profile_picture_url"
    )

  Assert-FileContainsPatterns `
    -Label 'chat GL profile context is self/admin scoped' `
    -Path (Join-Path $RoutesRoot 'chat.ts') `
    -Patterns @(
      'const authenticatedGlId = getAuthenticatedGlId\(req\.user\)',
      "req\.user\?\.role === 'admin'",
      '\? await resolveAdminChatContextIds\(authUserId, glId\)',
      "\? \{ authUserId: authenticatedGlId, glId: authenticatedGlId \}"
    )
}

function Assert-AccountLifecycleInvalidatesRouteAuth {
  param(
    [string]$RoutesRoot,
    [string]$MiddlewarePath
  )

  Assert-FileContainsPatterns `
    -Label 'GL deactivation blocks stale tokens through active profile check' `
    -Path $MiddlewarePath `
    -Patterns @(
      'const isActiveGlProfile = async',
      "\.from\('gebietsleiter'\)",
      "\.select\('id, is_active'\)",
      "profile\.role === 'gl'",
      'User account is inactive'
    )

  Assert-FileContainsPatterns `
    -Label 'route auth requires an explicit Bearer token scheme' `
    -Path $MiddlewarePath `
    -Patterns @(
      "authHeader === 'string'",
      "authHeader\.trim\(\)\.split\(/\\s\+/\)",
      "authParts\.length === 2 && authParts\[0\] === 'Bearer'"
    )

  Assert-PatternOrder `
    -Label 'GL active profile check happens before request user is trusted' `
    -Path $MiddlewarePath `
    -Before "profile\.role === 'gl'" `
    -After 'req\.user = normalizeProfile'

  Assert-FileContainsPatterns `
    -Label 'GL deactivation pseudonymizes personal profile data' `
    -Path (Join-Path $RoutesRoot 'gebietsleiter.ts') `
    -Patterns @(
      "router\.delete\('/:id',\s*requireAdmin",
      'removeProfilePictureObject\(freshClient, existingGl\?\.profile_picture_url\)',
      'is_active:\s*false',
      'name:\s*`Deleted GL \$\{id\.substring\(0, 8\)\}`',
      "address:\s*''",
      "postal_code:\s*''",
      "city:\s*''",
      "phone:\s*''",
      'profile_picture_url:\s*null',
      "password_hash:\s*'DEACTIVATED'"
    )

  Assert-NoMatches `
    -Label 'GL deactivation does not hard-delete reporting profile fallback' `
    -Path (Join-Path $RoutesRoot 'gebietsleiter.ts') `
    -Patterns @(
      'If update fails, try to delete',
      "\.from\('gebietsleiter'\)\s*[\r\n\s]*\.delete\(\)"
    )

  Assert-FileContainsPatterns `
    -Label 'admin deletion removes users profile used by route auth' `
    -Path (Join-Path $RoutesRoot 'auth.ts') `
    -Patterns @(
      "router\.delete\('/admin/:id',\s*authenticateToken,\s*requireAdmin",
      "supabase\.auth\.admin\.deleteUser\(id\)",
      "\.from\('users'\)",
      '\.delete\(\)',
      "\.eq\('id', id\)",
      'User profile not found'
    )

  Assert-FileContainsPatterns `
    -Label 'DSGVO README documents stale-token account lifecycle boundary' `
    -Path 'sql/README_DSGVO_RLS.md' `
    -Patterns @(
      'Account lifecycle changes must invalidate route-level authorization even if an old access token still exists',
      'admin deletion removes the `users` profile used by `authenticateToken`',
      'GL deactivation is blocked by the active `gebietsleiter` profile check'
    )
}

function Assert-HttpBoundaryIsHardened {
  param(
    [string]$IndexPath
  )

  Assert-FileContainsPatterns `
    -Label 'HTTP boundary has explicit CORS and security headers' `
    -Path $IndexPath `
    -Patterns @(
      "app\.disable\('x-powered-by'\)",
      "res\.setHeader\('X-Content-Type-Options', 'nosniff'\)",
      "res\.setHeader\('Referrer-Policy', 'no-referrer'\)",
      "res\.setHeader\('X-Frame-Options', 'DENY'\)",
      "res\.setHeader\('Cache-Control', 'no-store'\)",
      "const isProduction = process\.env\.NODE_ENV === 'production'",
      "const allowLocalCors = !isProduction \|\| process\.env\.ALLOW_LOCAL_CORS === 'true'",
      'const localCorsOrigins = allowLocalCors \? \[',
      'const allowedCorsOrigins = Array\.from\(new Set\(configuredCorsOrigins\)\)',
      'allowedCorsOrigins\.includes\(normalizedOrigin\)',
      "allowedHeaders: \['Authorization', 'Content-Type'\]",
      "exposedHeaders: \['Content-Disposition'\]",
      'const sanitizeLoggedPath = \(value: string\): string =>',
      'sanitizeLoggedPath\(req\.path\)'
    )

  Assert-NoMatches `
    -Label 'HTTP CORS does not allow wildcard or credentialed browser access' `
    -Path $IndexPath `
    -Patterns @(
      "origin:\s*['""]\*['""]",
      'credentials:\s*true'
    )

  Assert-FileContainsPatterns `
    -Label 'backend env docs keep localhost CORS production override disabled by default' `
    -Path 'ENV_TEMPLATE.md' `
    -Patterns @(
      'CORS_ORIGINS=https://mars-rover-mu\.vercel\.app',
      'Localhost CORS is enabled automatically outside NODE_ENV=production',
      'Only set this to true temporarily for a controlled production debugging window',
      'ALLOW_LOCAL_CORS=false'
    )

  Assert-FileContainsPatterns `
    -Label 'backend README documents production localhost CORS boundary' `
    -Path 'README.md' `
    -Patterns @(
      'CORS_ORIGINS=https://mars-rover-mu\.vercel\.app',
      'ALLOW_LOCAL_CORS=false',
      'Localhost CORS origins are enabled automatically outside `NODE_ENV=production`',
      'In production, keep `ALLOW_LOCAL_CORS` unset or `false` unless temporary local browser access is explicitly required',
      'Optional local-browser CORS override for production; keep `false` by default'
    )
}

function Assert-AuthEndpointsAreAbuseHardened {
  param(
    [string]$AuthRoutePath
  )

  Assert-FileContainsPatterns `
    -Label 'unauthenticated auth endpoints are rate limited' `
    -Path $AuthRoutePath `
    -Patterns @(
      'const AUTH_RATE_LIMIT_WINDOW_MS = 15 \* 60 \* 1000',
      'const AUTH_LOGIN_RATE_LIMIT_MAX_ATTEMPTS = 20',
      'const AUTH_REFRESH_RATE_LIMIT_MAX_ATTEMPTS = 120',
      'const AUTH_RATE_LIMIT_MAX_BUCKETS = 10000',
      'const sanitizeLoggedPath = \(value: string\): string =>',
      'createHash\(''sha256''\)\.update\(ip\)\.digest\(''hex''\)',
      'createHash\(''sha256''\)\.update\(username\)\.digest\(''hex''\)',
      'createHash\(''sha256''\)\.update\(refreshToken\)\.digest\(''hex''\)',
      'return `\$\{req\.path\}:\$\{ipHash\}:\$\{usernameHash \|\| refreshHash\}`',
      'authRateLimitBuckets\.size > AUTH_RATE_LIMIT_MAX_BUCKETS',
      'authRateLimitBuckets\.delete\(oldestKey\)',
      'sanitizeLoggedPath\(req\.path\)',
      'router\.post\(''/login'', authRateLimit',
      'router\.post\(''/refresh'', authRateLimit',
      'Too many authentication attempts'
    )

  Assert-FileContainsPatterns `
    -Label 'login failures do not disclose account existence' `
    -Path $AuthRoutePath `
    -Patterns @(
      'return res\.status\(401\)\.json\(\{ error: ''Invalid email or password'' \}\)'
    )
}

function Assert-WriteRoutesHaveReviewedAuthorization {
  param(
    [string]$RoutesRoot
  )

  $adminMountedRouteFiles = @(
    'actionHistory.ts',
    'activities.ts',
    'export.ts',
    'productsUpdate.ts'
  )

  $reviewedSelfScopedWriteRoutes = @(
    'auth.ts POST /login',
    'auth.ts POST /refresh',
    'auth.ts POST /logout',
    'bugReports.ts POST /upload',
    'bugReports.ts POST /',
    'chat.ts POST /',
    'fragebogen.ts POST /responses/upload-photo',
    'fragebogen.ts POST /responses/photo-url',
    'fragebogen.ts POST /responses',
    'fragebogen.ts POST /zeiterfassung',
    'fragebogen.ts POST /zusatz-zeiterfassung',
    'maps.ts POST /driving-times',
    'maps.ts POST /optimize-route',
    'markets.ts POST /:id/visit',
    'naraIncentive.ts POST /',
    'vorverkauf.ts POST /',
    'vorverkauf.ts POST /submit',
    'vorverkaufWellen.ts POST /submit',
    'wellen.ts POST /photos/upload',
    'wellen.ts POST /upload-delivery-photo',
    'wellen.ts POST /upload-delivery-photos-per-item'
  )

  $routePattern = [regex]'router\.(post|put|patch|delete)\(\s*(["''])(?<route>.*?)\2\s*,(?<middleware>.*?)async\s*\('
  $violations = @()
  $discoveredRouteKeys = New-Object System.Collections.Generic.HashSet[string]

  Get-ChildItem -Path $RoutesRoot -File -Filter '*.ts' | ForEach-Object {
    $fileName = $_.Name
    $text = Get-Content -Raw $_.FullName
    foreach ($match in $routePattern.Matches($text)) {
      $method = $match.Groups[1].Value.ToUpperInvariant()
      $route = $match.Groups['route'].Value
      $middleware = $match.Groups['middleware'].Value
      $routeKey = "$fileName $method $route"
      [void]$discoveredRouteKeys.Add($routeKey)

      $hasRouteGuard = $middleware -match 'require[A-Za-z0-9_]*(Admin|Owner|Self|GL)'
      $isAdminMounted = $adminMountedRouteFiles -contains $fileName
      $isReviewedSelfScoped = $reviewedSelfScopedWriteRoutes -contains $routeKey

      if (!$hasRouteGuard -and !$isAdminMounted -and !$isReviewedSelfScoped) {
        $lineNumber = ($text.Substring(0, $match.Index) -split "`r?`n").Count
        $violations += "$($_.FullName):$lineNumber $routeKey"
      }
    }
  }

  foreach ($routeKey in $reviewedSelfScopedWriteRoutes) {
    if (!$discoveredRouteKeys.Contains($routeKey)) {
      $violations += "reviewed write route allowlist entry does not match a current route: $routeKey"
    }
  }

  if ($violations.Count -gt 0) {
    Write-Host 'FAIL service-role write routes have reviewed authorization'
    $violations | ForEach-Object { Write-Host ("  " + $_) }
    throw 'unreviewed service-role write route authorization'
  }

  Write-Host 'PASS service-role write routes have reviewed authorization'
}

function Assert-AllRouteDefinitionsAreAuditable {
  param(
    [string]$RoutesRoot
  )

  $routePattern = [regex]'router\.(get|post|put|patch|delete)\(\s*(["''])(?<route>.*?)\2\s*,'
  $violations = @()

  Get-ChildItem -Path $RoutesRoot -File -Filter '*.ts' | ForEach-Object {
    $fileName = $_.Name
    $text = Get-Content -Raw $_.FullName
    foreach ($match in $routePattern.Matches($text)) {
      $routeWindow = $text.Substring($match.Index, [Math]::Min(600, $text.Length - $match.Index))
      if ($routeWindow -notmatch 'async\s*\(') {
        $lineNumber = ($text.Substring(0, $match.Index) -split "`r?`n").Count
        $method = $match.Groups[1].Value.ToUpperInvariant()
        $route = $match.Groups['route'].Value
        $violations += "$($_.FullName):$lineNumber $fileName $method $route"
      }
    }
  }

  if ($violations.Count -gt 0) {
    Write-Host 'FAIL route authorization audit can see every route handler'
    $violations | ForEach-Object { Write-Host ("  " + $_) }
    throw 'route handler audit coverage failed'
  }

  Write-Host 'PASS route authorization audit can see every route handler'
}

function Assert-ReadRoutesHaveReviewedAuthorization {
  param(
    [string]$RoutesRoot
  )

  $adminMountedRouteFiles = @(
    'actionHistory.ts',
    'activities.ts',
    'export.ts',
    'productsUpdate.ts'
  )

  $reviewedAuthenticatedReadRoutes = @(
    'auth.ts GET /me',
    'fragebogen.ts GET /questions',
    'fragebogen.ts GET /questions/:id',
    'fragebogen.ts GET /modules',
    'fragebogen.ts GET /modules/:id',
    'fragebogen.ts GET /fragebogen',
    'fragebogen.ts GET /fragebogen/:id',
    'fragebogen.ts GET /responses/completed-map',
    'markets.ts GET /',
    'markets.ts GET /:id',
    'markets.ts GET /:id/history',
    'markets.ts GET /:id/visit-crm',
    'naraIncentive.ts GET /',
    'products.ts GET /',
    'products.ts GET /:id',
    'vorverkauf.ts GET /',
    'vorverkaufWellen.ts GET /',
    'vorverkaufWellen.ts GET /:id',
    'vorverkaufWellen.ts GET /:id/markets',
    'wellen.ts GET /dashboard/chain-averages',
    'wellen.ts GET /dashboard/waves',
    'wellen.ts GET /',
    'wellen.ts GET /:id',
    'wellen.ts GET /market/:marketId/pending-photos'
  )

  $routePattern = [regex]'router\.get\(\s*(["''])(?<route>.*?)\1\s*,(?<middleware>.*?)async\s*\('
  $violations = @()
  $discoveredRouteKeys = New-Object System.Collections.Generic.HashSet[string]

  Get-ChildItem -Path $RoutesRoot -File -Filter '*.ts' | ForEach-Object {
    $fileName = $_.Name
    $text = Get-Content -Raw $_.FullName
    foreach ($match in $routePattern.Matches($text)) {
      $route = $match.Groups['route'].Value
      $middleware = $match.Groups['middleware'].Value
      $routeKey = "$fileName GET $route"
      [void]$discoveredRouteKeys.Add($routeKey)

      $hasRouteGuard = $middleware -match 'require[A-Za-z0-9_]*(Admin|Owner|Self|GL)'
      $hasAuthRouterGuard = $fileName -eq 'auth.ts' -and $middleware -match 'authenticateToken'
      $isAdminMounted = $adminMountedRouteFiles -contains $fileName
      $isReviewedAuthenticatedRead = $reviewedAuthenticatedReadRoutes -contains $routeKey

      if (!$hasRouteGuard -and !$hasAuthRouterGuard -and !$isAdminMounted -and !$isReviewedAuthenticatedRead) {
        $lineNumber = ($text.Substring(0, $match.Index) -split "`r?`n").Count
        $violations += "$($_.FullName):$lineNumber $routeKey"
      }
    }
  }

  foreach ($routeKey in $reviewedAuthenticatedReadRoutes) {
    if (!$discoveredRouteKeys.Contains($routeKey)) {
      $violations += "reviewed read route allowlist entry does not match a current route: $routeKey"
    }
  }

  if ($violations.Count -gt 0) {
    Write-Host 'FAIL service-role read routes have reviewed authorization'
    $violations | ForEach-Object { Write-Host ("  " + $_) }
    throw 'unreviewed service-role read route authorization'
  }

  Write-Host 'PASS service-role read routes have reviewed authorization'
}

function Assert-AuthenticatedFetchInstalledBeforeApp {
  param(
    [string]$Path
  )

  $installImport = Select-String -Path $Path -Pattern "import \{ installAuthenticatedFetch \} from './services/apiFetch'" | Select-Object -First 1
  $installCall = Select-String -Path $Path -Pattern "installAuthenticatedFetch\(\)" | Select-Object -First 1
  $appImport = Select-String -Path $Path -Pattern "import App from './App" | Select-Object -First 1
  $renderCall = Select-String -Path $Path -Pattern "createRoot\(document\.getElementById\('root'\)!\)\.render" | Select-Object -First 1

  if (!$installImport -or !$installCall -or !$appImport -or !$renderCall -or $installCall.LineNumber -ge $renderCall.LineNumber) {
    Write-Host 'FAIL frontend authenticated fetch is installed before app render'
    throw 'frontend authenticated fetch install order failed'
  }

  Write-Host 'PASS frontend authenticated fetch is installed before app render'
}

function Get-FrontendSourceFiles {
  param(
    [string]$Root
  )

  $sourceFiles = Get-ChildItem -Path (Join-Path $Root 'src') -Recurse -File -Include *.ts,*.tsx |
    Select-Object -ExpandProperty FullName

  @(
    $sourceFiles
    Join-Path $Root 'package.json'
    Join-Path $Root 'package-lock.json'
  ) | Where-Object { Test-Path $_ }
}

function Invoke-CheckedNpm {
  param(
    [string[]]$Arguments
  )

  & npm.cmd @Arguments
  if ($LASTEXITCODE -ne 0) {
    throw "npm $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
  }
}

function Assert-SupabaseObjectCoverage {
  param(
    [string]$SourceRoot,
    [string]$SqlPath
  )

  $storageBuckets = @(
    'bug-screenshots',
    'fragebogen-response-images',
    'gl-profile-pictures',
    'question-images',
    'vorbesteller-lieferung',
    'wellen-images',
    'wellen-photos'
  )

  $sourceFiles = Get-ChildItem -Path $SourceRoot -Recurse -File -Include *.ts |
    Select-Object -ExpandProperty FullName

  $referencedObjects = Select-String -Path $sourceFiles -Pattern "\.from\('([^']+)'\)" -AllMatches |
    ForEach-Object { $_.Matches } |
    ForEach-Object { $_.Groups[1].Value } |
    Where-Object { $storageBuckets -notcontains $_ } |
    Sort-Object -Unique

  $sqlText = Get-Content -Raw $SqlPath
  $hardenedObjects = New-Object System.Collections.Generic.HashSet[string]
  $hardeningTableListMatch = [regex]::Match(
    $sqlText,
    '-- Enable RLS on every public table[\s\S]*?foreach t in array array\[(?<tables>[\s\S]*?)\]\s*loop',
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
  )
  if ($hardeningTableListMatch.Success) {
    foreach ($match in [regex]::Matches($hardeningTableListMatch.Groups['tables'].Value, "'public\.([a-zA-Z_][\w]*)'", 'IgnoreCase')) {
      [void]$hardenedObjects.Add($match.Groups[1].Value)
    }
  }

  $protectedViewListMatch = [regex]::Match(
    $sqlText,
    '-- Public views bypass[\s\S]*?foreach v in array array\[(?<views>[\s\S]*?)\]\s*loop',
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
  )
  if ($protectedViewListMatch.Success) {
    foreach ($match in [regex]::Matches($protectedViewListMatch.Groups['views'].Value, "'public\.([a-zA-Z_][\w]*)'", 'IgnoreCase')) {
      [void]$hardenedObjects.Add($match.Groups[1].Value)
    }
  }

  $missingObjects = @()
  foreach ($objectName in $referencedObjects) {
    if (!$hardenedObjects.Contains($objectName)) {
      $missingObjects += $objectName
    }
  }

  if ($missingObjects.Count -gt 0) {
    Write-Host 'FAIL backend Supabase objects are covered by RLS hardening SQL'
    $missingObjects | ForEach-Object { Write-Host ("  missing: " + $_) }
    throw 'backend Supabase object coverage failed'
  }

  Write-Host 'PASS backend Supabase objects are covered by RLS hardening SQL'
}

function Assert-DynamicSupabaseFromCallsAreBounded {
  param(
    [string]$SourceRoot
  )

  $sourceFiles = Get-ChildItem -Path $SourceRoot -Recurse -File -Include *.ts |
    Select-Object -ExpandProperty FullName

  $allowedDynamicArgs = @('tableName', 'table', 'primaryBucket')
  $dynamicArgViolations = @()

  $dynamicFromMatches = Select-String `
    -Path $sourceFiles `
    -Pattern '(?<!Array)(?<!Buffer)\.from\(\s*([a-z_][a-zA-Z0-9_]*)\s*\)' `
    -AllMatches `
    -CaseSensitive

  foreach ($matchInfo in $dynamicFromMatches) {
    foreach ($match in $matchInfo.Matches) {
      $argName = $match.Groups[1].Value
      if ($allowedDynamicArgs -notcontains $argName) {
        $dynamicArgViolations += "$($matchInfo.Path):$($matchInfo.LineNumber) dynamic .from($argName) is not reviewed"
        continue
      }

      $normalizedPath = $matchInfo.Path.Replace('\', '/')
      if ($argName -eq 'tableName' -and $normalizedPath -notmatch '/src/middleware/auth\.ts$') {
        $dynamicArgViolations += "$($matchInfo.Path):$($matchInfo.LineNumber) .from(tableName) is only reviewed in auth middleware"
      }
      if ($argName -eq 'table' -and $normalizedPath -notmatch '/src/(utils/exportTransformers|routes/(gebietsleiter|fragebogen|wellen))\.ts$') {
        $dynamicArgViolations += "$($matchInfo.Path):$($matchInfo.LineNumber) .from(table) is only reviewed in bounded lookup helpers"
      }
      if ($argName -eq 'primaryBucket' -and $normalizedPath -notmatch '/src/routes/wellen\.ts$') {
        $dynamicArgViolations += "$($matchInfo.Path):$($matchInfo.LineNumber) .from(primaryBucket) is only reviewed in Wellen photo signing/removal"
      }
    }
  }

  $callViolations = @()
  $allText = ($sourceFiles | ForEach-Object { Get-Content -Raw $_ }) -join "`n"

  foreach ($match in [regex]::Matches($allText, 'requireOwnedRowOrAdmin\(\s*(?<arg>[^,\)\r\n]+)', 'IgnoreCase')) {
    if ($match.Groups['arg'].Value.Trim() -notmatch "^'[^']+'$") {
      $callViolations += "requireOwnedRowOrAdmin first argument must be a literal table name: $($match.Groups['arg'].Value.Trim())"
    }
  }

  $boundedLookupHelpers = @(
    'fetchRowsByIdsInChunks',
    'fetchValueMap',
    'fetchRowsByIdChunks',
    'fetchAdminRowsByIdChunks'
  )

  foreach ($helperName in $boundedLookupHelpers) {
    foreach ($match in [regex]::Matches($allText, "$helperName\((?<args>[^;\n]+)", 'IgnoreCase')) {
      $prefixStart = [Math]::Max(0, $match.Index - 40)
      $prefix = $allText.Substring($prefixStart, $match.Index - $prefixStart)
      if ($prefix -match '(async\s+)?function\s+$') {
        continue
      }

      $args = $match.Groups['args'].Value.Split(',')
      if ($args.Count -lt 2 -or $args[1].Trim() -notmatch "^'[^']+'$") {
        $callViolations += "$helperName second argument must be a literal table name: $($match.Groups['args'].Value.Trim())"
      }
    }
  }

  $wellenRouteText = Get-Content -Raw (Join-Path $SourceRoot 'routes/wellen.ts')
  $requiredBucketBinding = 'const primaryBucket = privatePath ? WELLEN_PHOTOS_BUCKET : WELLEN_IMAGES_BUCKET;'
  $primaryBucketAssignments = [regex]::Matches($wellenRouteText, 'const\s+primaryBucket\s*=')
  if ($primaryBucketAssignments.Count -ne 2 -or $wellenRouteText -notlike "*$requiredBucketBinding*") {
    $callViolations += 'primaryBucket must stay bound to the reviewed WELLEN_PHOTOS_BUCKET/WELLEN_IMAGES_BUCKET pair'
  }

  if ($dynamicArgViolations.Count -gt 0 -or $callViolations.Count -gt 0) {
    Write-Host 'FAIL dynamic Supabase .from calls are bounded'
    $dynamicArgViolations | ForEach-Object { Write-Host ("  " + $_) }
    $callViolations | ForEach-Object { Write-Host ("  " + $_) }
    throw 'dynamic Supabase .from boundary audit failed'
  }

  Write-Host 'PASS dynamic Supabase .from calls are bounded'
}

function Assert-LocalSchemaObjectsCovered {
  param(
    [string]$SchemaRoot,
    [string]$SqlPath
  )

  $schemaFiles = Get-ChildItem -Path $SchemaRoot -File -Filter '*.sql' |
    Where-Object { $_.Name -match '^(database|databse)_schema.*\.sql$' }

  $tableNames = New-Object System.Collections.Generic.HashSet[string]
  $viewNames = New-Object System.Collections.Generic.HashSet[string]
  $functionNames = New-Object System.Collections.Generic.HashSet[string]

  foreach ($file in $schemaFiles) {
    $text = Get-Content -Raw $file.FullName
    foreach ($match in [regex]::Matches($text, 'create\s+table\s+(?:if\s+not\s+exists\s+)?(?:public\.)?([a-zA-Z_][\w]*)', 'IgnoreCase')) {
      [void]$tableNames.Add($match.Groups[1].Value)
    }
    foreach ($match in [regex]::Matches($text, 'create\s+(?:or\s+replace\s+)?view\s+(?:public\.)?([a-zA-Z_][\w]*)', 'IgnoreCase')) {
      [void]$viewNames.Add($match.Groups[1].Value)
    }
    foreach ($match in [regex]::Matches($text, 'create\s+(?:or\s+replace\s+)?function\s+(?:public\.)?([a-zA-Z_][\w]*)', 'IgnoreCase')) {
      [void]$functionNames.Add($match.Groups[1].Value)
    }
  }

  $sqlText = Get-Content -Raw $SqlPath
  $missing = @()
  $hardeningTableListMatch = [regex]::Match(
    $sqlText,
    '-- Enable RLS on every public table[\s\S]*?foreach t in array array\[(?<tables>[\s\S]*?)\]\s*loop',
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
  )

  $hardeningTableNames = New-Object System.Collections.Generic.HashSet[string]
  if ($hardeningTableListMatch.Success) {
    foreach ($match in [regex]::Matches($hardeningTableListMatch.Groups['tables'].Value, "'public\.([a-zA-Z_][\w]*)'", 'IgnoreCase')) {
      [void]$hardeningTableNames.Add($match.Groups[1].Value)
    }
  }

  foreach ($tableName in ($tableNames | Sort-Object)) {
    if ($sqlText -notlike "*public.$tableName*" -and $sqlText -notlike "*('$tableName'*") {
      $missing += "table:$tableName"
    }
    if (!$hardeningTableListMatch.Success -or !$hardeningTableNames.Contains($tableName)) {
      $missing += "table-not-in-hardening-rls-list:$tableName"
    }
  }
  foreach ($viewName in ($viewNames | Sort-Object)) {
    if ($sqlText -notlike "*public.$viewName*" -and $sqlText -notlike "*'$viewName'*") {
      $missing += "view:$viewName"
    }
  }
  foreach ($functionName in ($functionNames | Sort-Object)) {
    if ($sqlText -notlike "*public.$functionName*") {
      $missing += "function:$functionName"
    }
  }

  if ($missing.Count -gt 0) {
    Write-Host 'FAIL local schema objects are covered by RLS hardening SQL'
    $missing | ForEach-Object { Write-Host ("  missing: " + $_) }
    throw 'local schema object coverage failed'
  }

  Write-Host 'PASS local schema objects are covered by RLS hardening SQL'
}

function Get-LocalSchemaTableColumns {
  param(
    [string]$SchemaRoot
  )

  $schemaFiles = Get-ChildItem -Path $SchemaRoot -File -Filter '*.sql' |
    Where-Object { $_.Name -match '^(database|databse)_schema.*\.sql$' }

  $tableColumns = @{}

  foreach ($file in $schemaFiles) {
    $text = Get-Content -Raw $file.FullName

    foreach ($match in [regex]::Matches($text, 'create\s+table\s+(?:if\s+not\s+exists\s+)?(?:public\.)?([a-zA-Z_][\w]*)\s*\((?<body>[\s\S]*?)\);', 'IgnoreCase')) {
      $tableName = $match.Groups[1].Value
      if (!$tableColumns.ContainsKey($tableName)) {
        $tableColumns[$tableName] = New-Object System.Collections.Generic.HashSet[string]
      }

      $bodyLines = $match.Groups['body'].Value -split "`r?`n"
      foreach ($line in $bodyLines) {
        $trimmedLine = ($line -replace '--.*$', '').Trim()
        if ($trimmedLine -match '^"?([a-zA-Z_][\w]*)"?\s+' -and
            $matches[1] -notmatch '^(constraint|primary|foreign|unique|check|exclude)$') {
          [void]$tableColumns[$tableName].Add($matches[1])
        }
      }
    }

    foreach ($match in [regex]::Matches($text, 'alter\s+table\s+(?:if\s+exists\s+)?(?:public\.)?([a-zA-Z_][\w]*)\s+add\s+column\s+(?:if\s+not\s+exists\s+)?"?([a-zA-Z_][\w]*)"?', 'IgnoreCase')) {
      $tableName = $match.Groups[1].Value
      $columnName = $match.Groups[2].Value
      if (!$tableColumns.ContainsKey($tableName)) {
        $tableColumns[$tableName] = New-Object System.Collections.Generic.HashSet[string]
      }
      [void]$tableColumns[$tableName].Add($columnName)
    }
  }

  return $tableColumns
}

function Assert-RlsPolicyColumnAssumptionsCovered {
  param(
    [string]$SchemaRoot
  )

  $tableColumns = Get-LocalSchemaTableColumns -SchemaRoot $SchemaRoot
  $expectedColumns = @{
    'users' = @('id', 'gebietsleiter_id')
    'gebietsleiter' = @('id', 'is_active')
    'products' = @('is_deleted')
    'bug_reports' = @('gebietsleiter_id')
    'gl_onboarding_reads' = @('gl_id')
    'fb_questions' = @('is_deleted')
    'fb_modules' = @('is_deleted')
    'fb_fragebogen' = @('is_deleted')
    'fb_zeiterfassung_submissions' = @('gebietsleiter_id')
    'fb_responses' = @('id', 'gebietsleiter_id')
    'fb_zusatz_zeiterfassung' = @('gebietsleiter_id')
    'fb_day_tracking' = @('gebietsleiter_id')
    'zeiterfassung_wochen_checks' = @('gebietsleiter_id')
    'wellen_gl_progress' = @('gebietsleiter_id')
    'wellen_submissions' = @('gebietsleiter_id')
    'wellen_photos' = @('gebietsleiter_id')
    'vorverkauf_entries' = @('id', 'gebietsleiter_id')
    'vorverkauf_submissions' = @('id', 'gebietsleiter_id')
    'nara_incentive_submissions' = @('id', 'gebietsleiter_id')
    'market_visits' = @('gebietsleiter_id')
    'fb_response_answers' = @('response_id')
    'vorverkauf_items' = @('vorverkauf_entry_id')
    'vorverkauf_submission_products' = @('submission_id')
    'nara_incentive_items' = @('submission_id')
  }

  $violations = @()
  foreach ($tableName in ($expectedColumns.Keys | Sort-Object)) {
    if (!$tableColumns.ContainsKey($tableName)) {
      $violations += "missing local schema table: $tableName"
      continue
    }

    foreach ($columnName in $expectedColumns[$tableName]) {
      if (!$tableColumns[$tableName].Contains($columnName)) {
        $violations += "missing local schema column: $tableName.$columnName"
      }
    }
  }

  if ($violations.Count -gt 0) {
    Write-Host 'FAIL RLS policy column assumptions exist in local schema files'
    $violations | ForEach-Object { Write-Host ("  " + $_) }
    throw 'RLS policy column assumption coverage failed'
  }

  Write-Host 'PASS RLS policy column assumptions exist in local schema files'
}

function Assert-MarketVisitSourceValuesCovered {
  param(
    [string]$RoutesRoot,
    [string]$SchemaRoot
  )

  $routeFiles = Get-ChildItem -Path $RoutesRoot -File -Filter '*.ts' |
    Select-Object -ExpandProperty FullName
  $routeText = ($routeFiles | ForEach-Object { Get-Content -Raw $_ }) -join "`n"
  $schemaText = Get-Content -Raw (Join-Path $SchemaRoot 'database_schema_market_visits.sql')

  $routeSources = [regex]::Matches(
    $routeText,
    "\.from\('market_visits'\)[\s\S]{0,700}?source:\s*'([^']+)'",
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
  ) | ForEach-Object { $_.Groups[1].Value } | Sort-Object -Unique

  $schemaSourcesMatch = [regex]::Match(
    $schemaText,
    "source\s+varchar\(\d+\)\s+not\s+null\s+check\s*\(\s*source\s+in\s*\((?<values>[^)]*)\)\s*\)",
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
  )

  if (!$schemaSourcesMatch.Success) {
    Write-Host 'FAIL market_visits source constraint is locally documented'
    throw 'market_visits source constraint missing from local schema'
  }

  $schemaSources = [regex]::Matches($schemaSourcesMatch.Groups['values'].Value, "'([^']+)'") |
    ForEach-Object { $_.Groups[1].Value } | Sort-Object -Unique

  $missingSources = @()
  foreach ($source in $routeSources) {
    if ($schemaSources -notcontains $source) {
      $missingSources += $source
    }
  }

  if ($missingSources.Count -gt 0) {
    Write-Host 'FAIL market_visits route source values are covered by local schema constraint'
    $missingSources | ForEach-Object { Write-Host ("  missing source: " + $_) }
    throw 'market_visits source constraint coverage failed'
  }

  Write-Host 'PASS market_visits route source values are covered by local schema constraint'
}

function Assert-StorageBucketsReferencedByCodeAreReviewed {
  param(
    [string]$SourceRoot,
    [string]$HardeningSqlPath,
    [string]$VerifierSqlPath
  )

  $sourceFiles = Get-ChildItem -Path $SourceRoot -Recurse -File -Include *.ts |
    Select-Object -ExpandProperty FullName
  $sourceText = ($sourceFiles | ForEach-Object { Get-Content -Raw $_ }) -join "`n"
  $hardeningText = Get-Content -Raw $HardeningSqlPath
  $verifierText = Get-Content -Raw $VerifierSqlPath

  $bucketConstants = @{}
  foreach ($match in [regex]::Matches($sourceText, "const\s+([A-Z0-9_]*BUCKET)\s*=\s*'([^']+)'")) {
    $bucketConstants[$match.Groups[1].Value] = $match.Groups[2].Value
  }

  $bucketVariables = @{}
  foreach ($match in [regex]::Matches($sourceText, "const\s+([A-Za-z0-9_]*Bucket)\s*=\s*[^?;\r\n]+\?\s*([^:;\r\n]+)\s*:\s*([^;\r\n]+)")) {
    $variableName = $match.Groups[1].Value
    $resolvedBuckets = @()
    foreach ($branchExpression in @($match.Groups[2].Value.Trim(), $match.Groups[3].Value.Trim())) {
      if ($branchExpression -match "^'([^']+)'$") {
        $resolvedBuckets += $Matches[1]
      } elseif ($bucketConstants.ContainsKey($branchExpression)) {
        $resolvedBuckets += $bucketConstants[$branchExpression]
      }
    }

    if ($resolvedBuckets.Count -eq 2) {
      $bucketVariables[$variableName] = $resolvedBuckets
    }
  }

  $referencedBuckets = New-Object System.Collections.Generic.HashSet[string]
  foreach ($match in [regex]::Matches($sourceText, "\.storage\s*\.\s*from\s*\(\s*'([^']+)'\s*\)")) {
    [void]$referencedBuckets.Add($match.Groups[1].Value)
  }
  foreach ($match in [regex]::Matches($sourceText, "\.storage\s*\.\s*from\s*\(\s*([A-Za-z0-9_]+)\s*\)")) {
    $bucketExpression = $match.Groups[1].Value
    if ($bucketConstants.ContainsKey($bucketExpression)) {
      [void]$referencedBuckets.Add($bucketConstants[$bucketExpression])
    } elseif ($bucketVariables.ContainsKey($bucketExpression)) {
      foreach ($bucketName in $bucketVariables[$bucketExpression]) {
        [void]$referencedBuckets.Add($bucketName)
      }
    }
  }
  foreach ($match in [regex]::Matches($sourceText, "\.storage\s*\.\s*(?:createBucket|updateBucket)\s*\(\s*([A-Z0-9_]*BUCKET|'[^']+')")) {
    $bucketExpression = $match.Groups[1].Value
    if ($bucketExpression -match "^'([^']+)'$") {
      [void]$referencedBuckets.Add($Matches[1])
    } elseif ($bucketConstants.ContainsKey($bucketExpression)) {
      [void]$referencedBuckets.Add($bucketConstants[$bucketExpression])
    }
  }

  $verifierReviewedBuckets = New-Object System.Collections.Generic.HashSet[string]
  $verifierReviewedBucketsBlock = [regex]::Match(
    $verifierText,
    'with\s+expected_reviewed_storage_buckets\(bucket_id\)\s+as\s*\(\s*values(?<items>[\s\S]*?)^\s*\)\s*select',
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase -bor [System.Text.RegularExpressions.RegexOptions]::Multiline
  )
  if ($verifierReviewedBucketsBlock.Success) {
    foreach ($match in [regex]::Matches($verifierReviewedBucketsBlock.Groups['items'].Value, "'([^']+)'")) {
      [void]$verifierReviewedBuckets.Add($match.Groups[1].Value)
    }
  }

  $violations = @()
  foreach ($bucketName in ($referencedBuckets | Sort-Object)) {
    if ($hardeningText -notmatch [regex]::Escape("'$bucketName'")) {
      $violations += "hardening SQL missing reviewed bucket: $bucketName"
    }
    if ($verifierText -notmatch [regex]::Escape("'$bucketName'")) {
      $violations += "metadata verifier missing reviewed bucket: $bucketName"
    }
    if (!$verifierReviewedBucketsBlock.Success -or !$verifierReviewedBuckets.Contains($bucketName)) {
      $violations += "metadata verifier expected_reviewed_storage_buckets missing referenced bucket: $bucketName"
    }
  }

  if ($violations.Count -gt 0) {
    Write-Host 'FAIL backend Storage buckets are explicitly reviewed by SQL and verifier'
    $violations | ForEach-Object { Write-Host ("  " + $_) }
    throw 'backend Storage bucket review coverage failed'
  }

  Write-Host 'PASS backend Storage buckets are explicitly reviewed by SQL and verifier'
}

function Assert-SignedStorageUrlsAreShortLived {
  param(
    [string]$SourceRoot
  )

  $sourceFiles = Get-ChildItem -Path $SourceRoot -Recurse -File -Include *.ts |
    Select-Object -ExpandProperty FullName
  $violations = @()

  foreach ($filePath in $sourceFiles) {
    $text = Get-Content -Raw $filePath
    $signedUrlConstants = @{}

    foreach ($match in [regex]::Matches($text, 'const\s+([A-Z0-9_]*SIGNED_URL_SECONDS)\s*=\s*([^;\r\n]+)')) {
      $constantName = $match.Groups[1].Value
      $expression = $match.Groups[2].Value.Trim()
      if ($expression -notmatch '^\d+(\s*\*\s*\d+)*$') {
        $lineNumber = ($text.Substring(0, $match.Index) -split "`r?`n").Count
        $violations += "${filePath}:$lineNumber signed URL expiry must be a numeric literal or multiplication expression: $constantName"
        continue
      }

      $value = 1
      foreach ($factor in ($expression -split '\*')) {
        $value *= [int]$factor.Trim()
      }

      $signedUrlConstants[$constantName] = $value
      if ($value -le 0 -or $value -gt 3600) {
        $lineNumber = ($text.Substring(0, $match.Index) -split "`r?`n").Count
        $violations += "${filePath}:$lineNumber signed URL expiry must be between 1 and 3600 seconds: $constantName=$value"
      }
    }

    foreach ($match in [regex]::Matches($text, '\.createSignedUrl\(\s*[^,\r\n]+,\s*([A-Za-z0-9_]+|\d+)\s*\)')) {
      $expiryArgument = $match.Groups[1].Value
      if ($expiryArgument -match '^\d+$') {
        $expiryValue = [int]$expiryArgument
        if ($expiryValue -le 0 -or $expiryValue -gt 3600) {
          $lineNumber = ($text.Substring(0, $match.Index) -split "`r?`n").Count
          $violations += "${filePath}:$lineNumber inline signed URL expiry must be between 1 and 3600 seconds"
        }
        continue
      }

      if (!$signedUrlConstants.ContainsKey($expiryArgument)) {
        $lineNumber = ($text.Substring(0, $match.Index) -split "`r?`n").Count
        $violations += "${filePath}:$lineNumber createSignedUrl must use a reviewed *_SIGNED_URL_SECONDS constant"
      }
    }
  }

  if ($violations.Count -gt 0) {
    Write-Host 'FAIL private Storage signed URLs are short-lived and reviewed'
    $violations | ForEach-Object { Write-Host ("  " + $_) }
    throw 'private Storage signed URL expiry review failed'
  }

  Write-Host 'PASS private Storage signed URLs are short-lived and reviewed'
}

function Assert-SignedStorageUrlsUseReviewedBuckets {
  param(
    [string]$SourceRoot
  )

  $reviewedSignedUrlBuckets = @(
    'bug-screenshots',
    'fragebogen-response-images',
    'gl-profile-pictures',
    'vorbesteller-lieferung',
    'wellen-images',
    'wellen-photos'
  )
  $sourceFiles = Get-ChildItem -Path $SourceRoot -Recurse -File -Include *.ts |
    Select-Object -ExpandProperty FullName
  $violations = @()

  foreach ($filePath in $sourceFiles) {
    $text = Get-Content -Raw $filePath
    $bucketConstants = @{}
    $bucketVariables = @{}

    foreach ($constantMatch in [regex]::Matches($text, "const\s+([A-Z0-9_]*BUCKET)\s*=\s*'([^']+)'")) {
      $bucketConstants[$constantMatch.Groups[1].Value] = @($constantMatch.Groups[2].Value)
    }

    foreach ($variableMatch in [regex]::Matches($text, "const\s+([A-Za-z0-9_]*Bucket)\s*=\s*[^?;\r\n]+\?\s*([^:;\r\n]+)\s*:\s*([^;\r\n]+)")) {
      $variableName = $variableMatch.Groups[1].Value
      $resolvedBuckets = @()
      foreach ($branchExpression in @($variableMatch.Groups[2].Value.Trim(), $variableMatch.Groups[3].Value.Trim())) {
        if ($branchExpression -match "^'([^']+)'$") {
          $resolvedBuckets += $Matches[1]
        } elseif ($bucketConstants.ContainsKey($branchExpression)) {
          $resolvedBuckets += $bucketConstants[$branchExpression]
        }
      }

      if ($resolvedBuckets.Count -eq 2) {
        $bucketVariables[$variableName] = $resolvedBuckets
      }
    }

    foreach ($match in [regex]::Matches($text, "\.storage\s*\.\s*from\s*\(\s*(?<bucket>[^)]+?)\s*\)\s*\.\s*createSignedUrl\s*\(")) {
      $bucketExpression = $match.Groups['bucket'].Value.Trim()
      $lineNumber = ($text.Substring(0, $match.Index) -split "`r?`n").Count
      $bucketNames = @()

      if ($bucketExpression -match "^'([^']+)'$") {
        $bucketNames = @($Matches[1])
      } elseif ($bucketConstants.ContainsKey($bucketExpression)) {
        $bucketNames = $bucketConstants[$bucketExpression]
      } elseif ($bucketVariables.ContainsKey($bucketExpression)) {
        $bucketNames = $bucketVariables[$bucketExpression]
      } else {
        $violations += "${filePath}:$lineNumber createSignedUrl bucket is not locally reviewable: $bucketExpression"
        continue
      }

      foreach ($bucketName in $bucketNames) {
        if ($reviewedSignedUrlBuckets -notcontains $bucketName) {
          $violations += "${filePath}:$lineNumber createSignedUrl may only use reviewed signed URL buckets; found $bucketName"
        }
      }
    }
  }

  if ($violations.Count -gt 0) {
    Write-Host 'FAIL signed Storage URLs use only reviewed buckets'
    $violations | ForEach-Object { Write-Host ("  " + $_) }
    throw 'signed Storage URL bucket review failed'
  }

  Write-Host 'PASS signed Storage URLs use only reviewed buckets'
}

function Assert-PublicStorageUrlsUseReviewedPublicBuckets {
  param(
    [string]$SourceRoot
  )

  $reviewedPublicBuckets = @('question-images', 'wellen-images')
  $sourceFiles = Get-ChildItem -Path $SourceRoot -Recurse -File -Include *.ts |
    Select-Object -ExpandProperty FullName
  $violations = @()

  foreach ($filePath in $sourceFiles) {
    $text = Get-Content -Raw $filePath
    $bucketConstants = @{}

    foreach ($constantMatch in [regex]::Matches($text, "const\s+([A-Z0-9_]*BUCKET)\s*=\s*'([^']+)'")) {
      $bucketConstants[$constantMatch.Groups[1].Value] = $constantMatch.Groups[2].Value
    }

    foreach ($match in [regex]::Matches($text, "\.storage\s*\.\s*from\s*\(\s*(?<bucket>'[^']+'|[A-Z0-9_]+)\s*\)\s*\.\s*getPublicUrl\s*\(")) {
      $bucketExpression = $match.Groups['bucket'].Value
      $lineNumber = ($text.Substring(0, $match.Index) -split "`r?`n").Count
      $bucketName = $null

      if ($bucketExpression -match "^'([^']+)'$") {
        $bucketName = $Matches[1]
      } elseif ($bucketConstants.ContainsKey($bucketExpression)) {
        $bucketName = $bucketConstants[$bucketExpression]
      } else {
        $violations += "${filePath}:$lineNumber getPublicUrl bucket constant is not locally reviewable: $bucketExpression"
        continue
      }

      if ($reviewedPublicBuckets -notcontains $bucketName) {
        $violations += "${filePath}:$lineNumber getPublicUrl may only be used for reviewed public buckets; found $bucketName"
      }
    }
  }

  if ($violations.Count -gt 0) {
    Write-Host 'FAIL public Storage URLs use only reviewed public buckets'
    $violations | ForEach-Object { Write-Host ("  " + $_) }
    throw 'public Storage URL bucket review failed'
  }

  Write-Host 'PASS public Storage URLs use only reviewed public buckets'
}

function Assert-StorageMutationsUseReviewedBuckets {
  param(
    [string]$SourceRoot
  )

  $reviewedMutableBuckets = @(
    'bug-screenshots',
    'fragebogen-response-images',
    'gl-profile-pictures',
    'question-images',
    'vorbesteller-lieferung',
    'wellen-images',
    'wellen-photos'
  )
  $sourceFiles = Get-ChildItem -Path $SourceRoot -Recurse -File -Include *.ts |
    Select-Object -ExpandProperty FullName
  $violations = @()

  foreach ($filePath in $sourceFiles) {
    $text = Get-Content -Raw $filePath
    $bucketConstants = @{}
    $bucketVariables = @{}

    foreach ($constantMatch in [regex]::Matches($text, "const\s+([A-Z0-9_]*BUCKET)\s*=\s*'([^']+)'")) {
      $bucketConstants[$constantMatch.Groups[1].Value] = @($constantMatch.Groups[2].Value)
    }

    foreach ($variableMatch in [regex]::Matches($text, "const\s+([A-Za-z0-9_]*Bucket)\s*=\s*[^?;\r\n]+\?\s*([^:;\r\n]+)\s*:\s*([^;\r\n]+)")) {
      $variableName = $variableMatch.Groups[1].Value
      $resolvedBuckets = @()
      foreach ($branchExpression in @($variableMatch.Groups[2].Value.Trim(), $variableMatch.Groups[3].Value.Trim())) {
        if ($branchExpression -match "^'([^']+)'$") {
          $resolvedBuckets += $Matches[1]
        } elseif ($bucketConstants.ContainsKey($branchExpression)) {
          $resolvedBuckets += $bucketConstants[$branchExpression]
        }
      }

      if ($resolvedBuckets.Count -eq 2) {
        $bucketVariables[$variableName] = $resolvedBuckets
      }
    }

    foreach ($match in [regex]::Matches($text, "\.storage\s*\.\s*from\s*\(\s*(?<bucket>[^)]+?)\s*\)\s*\.\s*(?<operation>upload|remove)\s*\(", [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)) {
      $bucketExpression = $match.Groups['bucket'].Value.Trim()
      $operation = $match.Groups['operation'].Value
      $lineNumber = ($text.Substring(0, $match.Index) -split "`r?`n").Count
      $bucketNames = @()

      if ($bucketExpression -match "^'([^']+)'$") {
        $bucketNames = @($Matches[1])
      } elseif ($bucketConstants.ContainsKey($bucketExpression)) {
        $bucketNames = $bucketConstants[$bucketExpression]
      } elseif ($bucketVariables.ContainsKey($bucketExpression)) {
        $bucketNames = $bucketVariables[$bucketExpression]
      } else {
        $violations += "${filePath}:$lineNumber Storage $operation bucket is not locally reviewable: $bucketExpression"
        continue
      }

      foreach ($bucketName in $bucketNames) {
        if ($reviewedMutableBuckets -notcontains $bucketName) {
          $violations += "${filePath}:$lineNumber Storage $operation may only use reviewed mutable buckets; found $bucketName"
        }
      }
    }
  }

  if ($violations.Count -gt 0) {
    Write-Host 'FAIL Storage mutations use only reviewed buckets'
    $violations | ForEach-Object { Write-Host ("  " + $_) }
    throw 'Storage mutation bucket review failed'
  }

  Write-Host 'PASS Storage mutations use only reviewed buckets'
}

function Assert-RuntimeBucketConfigurationIsPrivateAndBounded {
  param(
    [string]$SourceRoot
  )

  $sourceFiles = Get-ChildItem -Path $SourceRoot -Recurse -File -Include *.ts |
    Select-Object -ExpandProperty FullName
  $violations = @()

  foreach ($filePath in $sourceFiles) {
    $text = Get-Content -Raw $filePath
    foreach ($match in [regex]::Matches($text, '\.storage\.(createBucket|updateBucket)\(\s*([A-Z0-9_]+|''[^'']+'')\s*,\s*\{(?<options>[\s\S]*?)\}\s*\)', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)) {
      $options = $match.Groups['options'].Value
      $lineNumber = ($text.Substring(0, $match.Index) -split "`r?`n").Count

      if ($options -notmatch 'public:\s*false') {
        $violations += "${filePath}:$lineNumber runtime bucket configuration must keep public:false"
      }
      if ($options -notmatch 'allowedMimeTypes:\s*[A-Z0-9_]+') {
        $violations += "${filePath}:$lineNumber runtime bucket configuration must use a reviewed MIME allowlist"
      }
      if ($options -notmatch 'fileSizeLimit:\s*[A-Z0-9_]+') {
        $violations += "${filePath}:$lineNumber runtime bucket configuration must use a reviewed file size limit"
      }
    }
  }

  if ($violations.Count -gt 0) {
    Write-Host 'FAIL runtime-created Storage buckets are private and bounded'
    $violations | ForEach-Object { Write-Host ("  " + $_) }
    throw 'runtime Storage bucket configuration review failed'
  }

  Write-Host 'PASS runtime-created Storage buckets are private and bounded'
}

function Assert-DocsCoverReviewedStorageBuckets {
  param(
    [string]$ReadmePath,
    [string]$HardeningSqlPath,
    [string]$VerifierSqlPath
  )

  $bucketRegex = "'(bug-screenshots|fragebogen-response-images|gl-profile-pictures|question-images|vorbesteller-lieferung|wellen-images|wellen-photos)'"
  $readmeText = Get-Content -Raw $ReadmePath
  $hardeningText = Get-Content -Raw $HardeningSqlPath
  $verifierText = Get-Content -Raw $VerifierSqlPath

  $reviewedBuckets = @(
    [regex]::Matches($hardeningText, $bucketRegex, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase) |
      ForEach-Object { $_.Groups[1].Value }
    [regex]::Matches($verifierText, $bucketRegex, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase) |
      ForEach-Object { $_.Groups[1].Value }
  ) | Sort-Object -Unique

  $violations = @()
  foreach ($bucketName in $reviewedBuckets) {
    if ($readmeText -notmatch [regex]::Escape($bucketName)) {
      $violations += "README missing reviewed bucket: $bucketName"
    }
  }

  if ($violations.Count -gt 0) {
    Write-Host 'FAIL DSGVO README documents every reviewed Storage bucket'
    $violations | ForEach-Object { Write-Host ("  " + $_) }
    throw 'DSGVO README Storage bucket coverage failed'
  }

  Write-Host 'PASS DSGVO README documents every reviewed Storage bucket'
}

function Assert-RlsVerifierCoversHardeningTables {
  param(
    [string]$HardeningSqlPath,
    [string]$VerifierSqlPath
  )

  $hardeningText = Get-Content -Raw $HardeningSqlPath
  $verifierText = Get-Content -Raw $VerifierSqlPath

  $hardeningBlockMatch = [regex]::Match(
    $hardeningText,
    '-- Enable RLS on every public table[\s\S]*?foreach t in array array\[(?<tables>[\s\S]*?)\]\s*loop',
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
  )
  $hardeningTableBlocks = [regex]::Matches(
    $hardeningText,
    'foreach\s+t\s+in\s+array\s+array\[(?<tables>[\s\S]*?)\]\s*loop',
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
  )

  if (!$hardeningBlockMatch.Success) {
    Write-Host 'FAIL metadata verifier covers all hardening app tables'
    throw 'could not find hardening app table list'
  }

  if ($hardeningTableBlocks.Count -ne 2) {
    Write-Host 'FAIL hardening app table loops stay in lockstep'
    Write-Host "  expected 2 app table loops, found $($hardeningTableBlocks.Count)"
    throw 'hardening app table loop inventory changed'
  }

  $hardeningTablesRaw = [regex]::Matches(
    $hardeningBlockMatch.Groups['tables'].Value,
    "'public\.([a-zA-Z_][\w]*)'",
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
  ) | ForEach-Object { $_.Groups[1].Value }

  $duplicateHardeningTables = $hardeningTablesRaw | Group-Object | Where-Object { $_.Count -gt 1 }
  if ($duplicateHardeningTables) {
    Write-Host 'FAIL hardening app table list has unique entries'
    $duplicateHardeningTables | ForEach-Object { Write-Host ("  duplicate: " + $_.Name) }
    throw 'hardening app table list has duplicate entries'
  }

  $hardeningTables = $hardeningTablesRaw | Sort-Object -Unique
  $hardeningLoopViolations = @()

  for ($i = 0; $i -lt $hardeningTableBlocks.Count; $i++) {
    $loopTablesRaw = [regex]::Matches(
      $hardeningTableBlocks[$i].Groups['tables'].Value,
      "'public\.([a-zA-Z_][\w]*)'",
      [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
    ) | ForEach-Object { $_.Groups[1].Value }

    foreach ($duplicateTable in ($loopTablesRaw | Group-Object | Where-Object { $_.Count -gt 1 })) {
      $hardeningLoopViolations += "hardening app table loop $($i + 1) duplicates $($duplicateTable.Name)"
    }

    $loopTables = $loopTablesRaw | Sort-Object -Unique
    foreach ($tableName in $hardeningTables) {
      if ($loopTables -notcontains $tableName) {
        $hardeningLoopViolations += "hardening app table loop $($i + 1) missing $tableName"
      }
    }

    foreach ($tableName in $loopTables) {
      if ($hardeningTables -notcontains $tableName) {
        $hardeningLoopViolations += "hardening app table loop $($i + 1) includes unexpected table $tableName"
      }
    }
  }

  if ($hardeningLoopViolations.Count -gt 0) {
    Write-Host 'FAIL hardening app table loops stay in lockstep'
    $hardeningLoopViolations | ForEach-Object { Write-Host ("  " + $_) }
    throw 'hardening app table loop drift failed'
  }

  Write-Host 'PASS hardening app table loops stay in lockstep'

  $verifierBlocks = [regex]::Matches(
    $verifierText,
    'with\s+app_tables\(table_name\)\s+as\s*\(\s*values(?<tables>[\s\S]*?)^\s*\)\s*(?:,|select)',
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase -bor [System.Text.RegularExpressions.RegexOptions]::Multiline
  )

  if ($verifierBlocks.Count -eq 0) {
    Write-Host 'FAIL metadata verifier covers all hardening app tables'
    throw 'could not find verifier app table list'
  }

  $violations = @()
  for ($i = 0; $i -lt $verifierBlocks.Count; $i++) {
    $verifierTablesRaw = [regex]::Matches(
      $verifierBlocks[$i].Groups['tables'].Value,
      "'([a-zA-Z_][\w]*)'",
      [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
    ) | ForEach-Object { $_.Groups[1].Value }

    $duplicateVerifierTables = $verifierTablesRaw | Group-Object | Where-Object { $_.Count -gt 1 }
    foreach ($duplicateTable in $duplicateVerifierTables) {
      $violations += "verifier app_tables block $($i + 1) duplicates $($duplicateTable.Name)"
    }

    $verifierTables = $verifierTablesRaw | Sort-Object -Unique

    foreach ($tableName in $hardeningTables) {
      if ($verifierTables -notcontains $tableName) {
        $violations += "verifier app_tables block $($i + 1) missing $tableName"
      }
    }

    foreach ($tableName in $verifierTables) {
      if ($hardeningTables -notcontains $tableName) {
        $violations += "verifier app_tables block $($i + 1) includes unhardened table $tableName"
      }
    }
  }

  if ($violations.Count -gt 0) {
    Write-Host 'FAIL metadata verifier covers all hardening app tables'
    $violations | ForEach-Object { Write-Host ("  " + $_) }
    throw 'metadata verifier hardening table coverage failed'
  }

  Write-Host 'PASS metadata verifier covers all hardening app tables'
}

function Assert-RlsVerifierCoversHardeningViewsFunctionsAndBuckets {
  param(
    [string]$HardeningSqlPath,
    [string]$VerifierSqlPath
  )

  $hardeningText = Get-Content -Raw $HardeningSqlPath
  $verifierText = Get-Content -Raw $VerifierSqlPath
  $violations = @()

  $hardeningViewsBlock = [regex]::Match(
    $hardeningText,
    '-- Public views bypass[\s\S]*?foreach v in array array\[(?<items>[\s\S]*?)\]\s*loop',
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
  )
  $verifierViewsBlock = [regex]::Match(
    $verifierText,
    'with\s+protected_views\(view_name\)\s+as\s*\(\s*values(?<items>[\s\S]*?)^\s*\)\s*select',
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase -bor [System.Text.RegularExpressions.RegexOptions]::Multiline
  )

  if (!$hardeningViewsBlock.Success -or !$verifierViewsBlock.Success) {
    $violations += 'could not find protected view lists'
  } else {
    $hardeningViewsRaw = [regex]::Matches($hardeningViewsBlock.Groups['items'].Value, "'public\.([a-zA-Z_][\w]*)'", 'IgnoreCase') |
      ForEach-Object { $_.Groups[1].Value }
    $verifierViewsRaw = [regex]::Matches($verifierViewsBlock.Groups['items'].Value, "'([a-zA-Z_][\w]*)'", 'IgnoreCase') |
      ForEach-Object { $_.Groups[1].Value }
    foreach ($duplicateView in ($hardeningViewsRaw | Group-Object | Where-Object { $_.Count -gt 1 })) {
      $violations += "hardening protected view list duplicates $($duplicateView.Name)"
    }
    foreach ($duplicateView in ($verifierViewsRaw | Group-Object | Where-Object { $_.Count -gt 1 })) {
      $violations += "verifier protected_views duplicates $($duplicateView.Name)"
    }
    $hardeningViews = $hardeningViewsRaw | Sort-Object -Unique
    $verifierViews = $verifierViewsRaw | Sort-Object -Unique
    foreach ($viewName in $hardeningViews) {
      if ($verifierViews -notcontains $viewName) {
        $violations += "verifier protected_views missing $viewName"
      }
    }
    foreach ($viewName in $verifierViews) {
      if ($hardeningViews -notcontains $viewName) {
        $violations += "verifier protected_views includes unhardened view $viewName"
      }
    }
  }

  $hardeningFunctionsBlock = [regex]::Match(
    $hardeningText,
    'foreach fn in array array\[(?<items>[\s\S]*?)\]\s*loop',
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
  )
  $verifierFunctionsBlock = [regex]::Match(
    $verifierText,
    'with\s+protected_functions\(function_name\)\s+as\s*\(\s*values(?<items>[\s\S]*?)^\s*\)\s*select',
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase -bor [System.Text.RegularExpressions.RegexOptions]::Multiline
  )

  if (!$hardeningFunctionsBlock.Success -or !$verifierFunctionsBlock.Success) {
    $violations += 'could not find protected function lists'
  } else {
    $hardeningFunctionsRaw = [regex]::Matches($hardeningFunctionsBlock.Groups['items'].Value, "'public\.([a-zA-Z_][\w]*)\(", 'IgnoreCase') |
      ForEach-Object { $_.Groups[1].Value }
    $verifierFunctionsRaw = [regex]::Matches($verifierFunctionsBlock.Groups['items'].Value, "'([a-zA-Z_][\w]*)'", 'IgnoreCase') |
      ForEach-Object { $_.Groups[1].Value }
    foreach ($duplicateFunction in ($hardeningFunctionsRaw | Group-Object | Where-Object { $_.Count -gt 1 })) {
      $violations += "hardening protected function list duplicates $($duplicateFunction.Name)"
    }
    foreach ($duplicateFunction in ($verifierFunctionsRaw | Group-Object | Where-Object { $_.Count -gt 1 })) {
      $violations += "verifier protected_functions duplicates $($duplicateFunction.Name)"
    }
    $hardeningFunctions = $hardeningFunctionsRaw | Sort-Object -Unique
    $verifierFunctions = $verifierFunctionsRaw | Sort-Object -Unique
    foreach ($functionName in $hardeningFunctions) {
      if ($verifierFunctions -notcontains $functionName) {
        $violations += "verifier protected_functions missing $functionName"
      }
    }
    foreach ($functionName in $verifierFunctions) {
      if ($hardeningFunctions -notcontains $functionName) {
        $violations += "verifier protected_functions includes unhardened function $functionName"
      }
    }
  }

  $bucketRegex = "'(bug-screenshots|fragebogen-response-images|gl-profile-pictures|question-images|vorbesteller-lieferung|wellen-images|wellen-photos)'"
  $hardeningBuckets = [regex]::Matches($hardeningText, $bucketRegex, 'IgnoreCase') |
    ForEach-Object { $_.Groups[1].Value } | Sort-Object -Unique
  $verifierBuckets = [regex]::Matches($verifierText, $bucketRegex, 'IgnoreCase') |
    ForEach-Object { $_.Groups[1].Value } | Sort-Object -Unique

  foreach ($bucketName in $hardeningBuckets) {
    if ($verifierBuckets -notcontains $bucketName) {
      $violations += "verifier storage bucket checks missing $bucketName"
    }
  }

  $verifierBucketListBlock = [regex]::Match(
    $verifierText,
    'with\s+expected_reviewed_storage_buckets\(bucket_id\)\s+as\s*\(\s*values(?<items>[\s\S]*?)^\s*\)\s*select',
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase -bor [System.Text.RegularExpressions.RegexOptions]::Multiline
  )
  if (!$verifierBucketListBlock.Success) {
    $violations += 'could not find verifier reviewed storage bucket list'
  } else {
    $verifierBucketListRaw = [regex]::Matches($verifierBucketListBlock.Groups['items'].Value, "'([^']+)'", 'IgnoreCase') |
      ForEach-Object { $_.Groups[1].Value }
    foreach ($duplicateBucket in ($verifierBucketListRaw | Group-Object | Where-Object { $_.Count -gt 1 })) {
      $violations += "verifier expected_reviewed_storage_buckets duplicates $($duplicateBucket.Name)"
    }
    $verifierBucketList = $verifierBucketListRaw | Sort-Object -Unique
    foreach ($bucketName in $hardeningBuckets) {
      if ($verifierBucketList -notcontains $bucketName) {
        $violations += "verifier expected_reviewed_storage_buckets missing $bucketName"
      }
    }
    foreach ($bucketName in $verifierBucketList) {
      if ($hardeningBuckets -notcontains $bucketName) {
        $violations += "verifier expected_reviewed_storage_buckets includes unreviewed bucket $bucketName"
      }
    }
  }

  if ($violations.Count -gt 0) {
    Write-Host 'FAIL metadata verifier covers hardening views, functions, and buckets'
    $violations | ForEach-Object { Write-Host ("  " + $_) }
    throw 'metadata verifier view/function/bucket coverage failed'
  }

  Write-Host 'PASS metadata verifier covers hardening views, functions, and buckets'
}

function Assert-RlsVerifierCoversHardeningPolicies {
  param(
    [string]$HardeningSqlPath,
    [string]$VerifierSqlPath
  )

  $hardeningText = Get-Content -Raw $HardeningSqlPath
  $verifierText = Get-Content -Raw $VerifierSqlPath

  $hardeningPolicies = [regex]::Matches(
    $hardeningText,
    'dsgvo_[a-zA-Z0-9_]+',
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
  ) | ForEach-Object { $_.Value } |
    Where-Object { $_ -notin @('dsgvo_bug_screenshots_no_direct_object_access') } |
    Sort-Object -Unique

  $violations = @()
  foreach ($policyName in $hardeningPolicies) {
    if ($verifierText -notmatch [regex]::Escape($policyName)) {
      $violations += "metadata verifier missing policy check for $policyName"
    }
  }

  if ($violations.Count -gt 0) {
    Write-Host 'FAIL metadata verifier covers all hardening policy names'
    $violations | ForEach-Object { Write-Host ("  " + $_) }
    throw 'metadata verifier policy coverage failed'
  }

  Write-Host 'PASS metadata verifier covers all hardening policy names'
}

function Assert-RlsVerifierPolicyShapeListMatchesExpectedPolicies {
  param(
    [string]$VerifierSqlPath
  )

  $verifierText = Get-Content -Raw $VerifierSqlPath
  $expectedPoliciesBlock = [regex]::Match(
    $verifierText,
    'with\s+expected_policies\(table_name,\s*policyname\)\s+as\s*\(\s*values(?<items>[\s\S]*?)^\s*\)\s*select',
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase -bor [System.Text.RegularExpressions.RegexOptions]::Multiline
  )
  $expectedShapesBlock = [regex]::Match(
    $verifierText,
    'with\s+expected_policy_shapes\(table_name,\s*policyname,\s*expected_cmd,\s*allow_true_predicate\)\s+as\s*\(\s*values(?<items>[\s\S]*?)^\s*\)\s*,\s*policy_shapes',
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase -bor [System.Text.RegularExpressions.RegexOptions]::Multiline
  )

  if (!$expectedPoliciesBlock.Success -or !$expectedShapesBlock.Success) {
    Write-Host 'FAIL metadata verifier policy shape list matches expected policy list'
    throw 'could not find verifier expected policy lists'
  }

  $extractPairs = {
    param([string]$BlockText)
    [regex]::Matches($BlockText, "\('([^']+)'\s*,\s*'([^']+)'", [System.Text.RegularExpressions.RegexOptions]::IgnoreCase) |
      ForEach-Object { "$($_.Groups[1].Value)|$($_.Groups[2].Value)" }
  }

  $expectedPoliciesRaw = @(& $extractPairs $expectedPoliciesBlock.Groups['items'].Value)
  $expectedShapesRaw = @(& $extractPairs $expectedShapesBlock.Groups['items'].Value)
  $expectedPolicies = $expectedPoliciesRaw | Sort-Object -Unique
  $expectedShapes = $expectedShapesRaw | Sort-Object -Unique
  $violations = @()

  foreach ($duplicatePolicy in ($expectedPoliciesRaw | Group-Object | Where-Object { $_.Count -gt 1 })) {
    $violations += "expected_policies duplicates $($duplicatePolicy.Name)"
  }

  foreach ($duplicateShape in ($expectedShapesRaw | Group-Object | Where-Object { $_.Count -gt 1 })) {
    $violations += "expected_policy_shapes duplicates $($duplicateShape.Name)"
  }

  foreach ($policyKey in $expectedPolicies) {
    if ($expectedShapes -notcontains $policyKey) {
      $violations += "expected_policy_shapes missing $policyKey"
    }
  }

  foreach ($policyKey in $expectedShapes) {
    if ($expectedPolicies -notcontains $policyKey) {
      $violations += "expected_policy_shapes has extra $policyKey"
    }
  }

  if ($violations.Count -gt 0) {
    Write-Host 'FAIL metadata verifier policy shape list matches expected policy list'
    $violations | ForEach-Object { Write-Host ("  " + $_) }
    throw 'metadata verifier policy shape list drift failed'
  }

  Write-Host 'PASS metadata verifier policy shape list matches expected policy list'
}

function Assert-PreflightStorageBucketListMatchesHardeningAndVerifier {
  param(
    [string]$HardeningSqlPath,
    [string]$VerifierSqlPath,
    [string]$PreflightSqlPath
  )

  $hardeningText = Get-Content -Raw $HardeningSqlPath
  $verifierText = Get-Content -Raw $VerifierSqlPath
  $preflightText = Get-Content -Raw $PreflightSqlPath
  $violations = @()

  $hardeningBucketRows = [regex]::Matches(
    $hardeningText,
    "\('([^']+)'\s*,\s*'\1'\s*,\s*(true|false)\)",
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
  ) | ForEach-Object { $_.Groups[1].Value }

  $verifierBucketBlock = [regex]::Match(
    $verifierText,
    'with\s+expected_reviewed_storage_buckets\(bucket_id\)\s+as\s*\(\s*values(?<items>[\s\S]*?)^\s*\)\s*select',
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase -bor [System.Text.RegularExpressions.RegexOptions]::Multiline
  )

  $preflightBucketBlock = [regex]::Match(
    $preflightText,
    'with\s+expected_reviewed_storage_buckets\(bucket_id,\s*expected_public\)\s+as\s*\(\s*values(?<items>[\s\S]*?)^\s*\)\s*select',
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase -bor [System.Text.RegularExpressions.RegexOptions]::Multiline
  )
  if (!$preflightBucketBlock.Success) {
    $preflightBucketBlock = [regex]::Match(
      $preflightText,
      'from\s+storage\.buckets\s+b\s+where\s+b\.id\s+in\s*\((?<items>[\s\S]*?)\)\s*order\s+by\s+b\.id',
      [System.Text.RegularExpressions.RegexOptions]::IgnoreCase -bor [System.Text.RegularExpressions.RegexOptions]::Multiline
    )
  }

  if ($preflightText -notmatch 'left\s+join\s+storage\.buckets\s+b\s+on\s+b\.id\s*=\s*e\.bucket_id') {
    $violations += 'preflight storage bucket snapshot must left join storage.buckets by expected bucket id'
  }
  if ($preflightText -notmatch 'exists_before_apply' -or $preflightText -notmatch 'expected_public') {
    $violations += 'preflight storage bucket snapshot must include exists_before_apply and expected_public metadata'
  }

  if (!$verifierBucketBlock.Success) {
    $violations += 'could not find verifier expected_reviewed_storage_buckets list'
  }
  if (!$preflightBucketBlock.Success) {
    $violations += 'could not find preflight storage bucket snapshot list'
  }

  $extractBucketList = {
    param([string]$BlockText)
    [regex]::Matches($BlockText, "'([^']+)'", [System.Text.RegularExpressions.RegexOptions]::IgnoreCase) |
      ForEach-Object { $_.Groups[1].Value }
  }

  $hardeningBucketsRaw = @($hardeningBucketRows)
  $verifierBucketsRaw = if ($verifierBucketBlock.Success) { @(& $extractBucketList $verifierBucketBlock.Groups['items'].Value) } else { @() }
  $preflightBucketsRaw = if ($preflightBucketBlock.Success) { @(& $extractBucketList $preflightBucketBlock.Groups['items'].Value) } else { @() }

  foreach ($duplicateBucket in ($hardeningBucketsRaw | Group-Object | Where-Object { $_.Count -gt 1 })) {
    $violations += "hardening bucket upsert duplicates $($duplicateBucket.Name)"
  }
  foreach ($duplicateBucket in ($verifierBucketsRaw | Group-Object | Where-Object { $_.Count -gt 1 })) {
    $violations += "verifier reviewed bucket list duplicates $($duplicateBucket.Name)"
  }
  foreach ($duplicateBucket in ($preflightBucketsRaw | Group-Object | Where-Object { $_.Count -gt 1 })) {
    $violations += "preflight bucket snapshot list duplicates $($duplicateBucket.Name)"
  }

  $hardeningBuckets = $hardeningBucketsRaw | Sort-Object -Unique
  $verifierBuckets = $verifierBucketsRaw | Sort-Object -Unique
  $preflightBuckets = $preflightBucketsRaw | Sort-Object -Unique

  foreach ($bucketName in $hardeningBuckets) {
    if ($verifierBuckets -notcontains $bucketName) {
      $violations += "verifier reviewed bucket list missing hardening bucket $bucketName"
    }
    if ($preflightBuckets -notcontains $bucketName) {
      $violations += "preflight bucket snapshot missing hardening bucket $bucketName"
    }
  }

  foreach ($bucketName in $verifierBuckets) {
    if ($hardeningBuckets -notcontains $bucketName) {
      $violations += "verifier reviewed bucket list includes bucket not in hardening upsert $bucketName"
    }
    if ($preflightBuckets -notcontains $bucketName) {
      $violations += "preflight bucket snapshot missing verifier bucket $bucketName"
    }
  }

  foreach ($bucketName in $preflightBuckets) {
    if ($hardeningBuckets -notcontains $bucketName) {
      $violations += "preflight bucket snapshot includes bucket not in hardening upsert $bucketName"
    }
    if ($verifierBuckets -notcontains $bucketName) {
      $violations += "preflight bucket snapshot includes bucket not in verifier list $bucketName"
    }
  }

  if ($violations.Count -gt 0) {
    Write-Host 'FAIL preflight Storage bucket list matches hardening and verifier'
    $violations | ForEach-Object { Write-Host ("  " + $_) }
    throw 'preflight Storage bucket list drift failed'
  }

  Write-Host 'PASS preflight Storage bucket list matches hardening and verifier'
}

function Assert-NoSqlCredentialSeeds {
  param(
    [string]$SchemaRoot
  )

  $sqlFiles = Get-ChildItem -Path $SchemaRoot -File -Filter '*.sql'
  if (($sqlFiles | Measure-Object).Count -eq 0) {
    Write-Host 'PASS local SQL files contain no credential seed operations'
    return
  }

  Assert-NoMatches `
    -Label 'local SQL files contain no credential seed operations' `
    -Path ($sqlFiles | Select-Object -ExpandProperty FullName) `
    -Patterns @(
      'admin123',
      'password123',
      'INSERT\s+INTO\s+users',
      'DELETE\s+FROM\s+users',
      'password_hash.*\$2[aby]\$'
    )
}

function Assert-NoLegacyBroadRlsInLocalSchemaFiles {
  param(
    [string]$SchemaRoot
  )

  $schemaFiles = Get-ChildItem -Path $SchemaRoot -File -Filter '*.sql' |
    Where-Object { $_.Name -match '^(database|databse)_schema.*\.sql$' }

  if (($schemaFiles | Measure-Object).Count -eq 0) {
    Write-Host 'PASS local schema files contain no legacy broad RLS snippets'
    return
  }

  Assert-NoMatches `
    -Label 'local schema files contain no legacy broad RLS snippets' `
    -Path ($schemaFiles | Select-Object -ExpandProperty FullName) `
    -Patterns @(
      'auth\.role\(',
      'Allow all operations for authenticated users',
      'Allow authenticated users to (insert|update|delete) gebietsleiter',
      'FOR\s+ALL\s+USING\s*\(\s*true\s*\)',
      'FOR\s+SELECT\s+USING\s*\(\s*true\s*\)',
      'Anyone can view'
    )
}

function Assert-EnvFilesIgnoredAndUntracked {
  param(
    [string]$FrontendRoot,
    [string]$BackendRoot
  )

  Assert-FileContainsPatterns `
    -Label 'root gitignore excludes real env files' `
    -Path (Join-Path $FrontendRoot '.gitignore') `
    -Patterns @(
      '(?m)^\.env\r?$',
      '(?m)^\.env\.local\r?$',
      '(?m)^\.env\.\*\.local\r?$'
    )

  Assert-FileContainsPatterns `
    -Label 'backend gitignore excludes real env files' `
    -Path (Join-Path $BackendRoot '.gitignore') `
    -Patterns @(
      '(?m)^\.env\r?$',
      '(?m)^\.env\.local\r?$',
      '(?m)^\.env\.\*\.local\r?$'
    )

  $trackedEnvFiles = @()
  foreach ($repoRoot in @($FrontendRoot, $BackendRoot)) {
    $tracked = & git -C $repoRoot ls-files
    if ($LASTEXITCODE -ne 0) {
      throw "git ls-files failed in $repoRoot"
    }

    $trackedEnvFiles += $tracked | Where-Object {
      $_ -match '(^|/)\.env($|\.)' -and $_ -notmatch '(^|/)\.env\.(example|sample|template)$'
    } | ForEach-Object { Join-Path $repoRoot $_ }
  }

  if ($trackedEnvFiles.Count -gt 0) {
    Write-Host 'FAIL real env files are not tracked'
    $trackedEnvFiles | ForEach-Object { Write-Host ("  " + $_) }
    throw 'tracked env file check failed'
  }

  Write-Host 'PASS real env files are ignored and untracked'
}

function Assert-HardeningSqlDoesNotMutateAppRows {
  param(
    [string]$HardeningSqlPath
  )

  $appTables = @(
    'users',
    'gebietsleiter',
    'gl_onboarding_reads',
    'markets',
    'products',
    'products_update',
    'action_history',
    'bug_reports',
    'fb_questions',
    'fb_modules',
    'fb_module_questions',
    'fb_module_rules',
    'fb_fragebogen',
    'fb_fragebogen_modules',
    'fb_fragebogen_markets',
    'fb_responses',
    'fb_response_answers',
    'fb_zeiterfassung_submissions',
    'fb_zusatz_zeiterfassung',
    'fb_day_tracking',
    'zeiterfassung_wochen_checks',
    'wellen',
    'wellen_displays',
    'wellen_kartonware',
    'wellen_einzelprodukte',
    'wellen_kw_days',
    'wellen_markets',
    'wellen_paletten',
    'wellen_paletten_products',
    'wellen_schuetten',
    'wellen_schuetten_products',
    'wellen_gl_progress',
    'wellen_submissions',
    'wellen_photos',
    'wellen_photo_tags',
    'vorverkauf_entries',
    'vorverkauf_items',
    'vorverkauf_wellen',
    'vorverkauf_wellen_markets',
    'vorverkauf_submissions',
    'vorverkauf_submission_products',
    'nara_incentive_submissions',
    'nara_incentive_items',
    'market_visits'
  )

  $tableAlternation = ($appTables | ForEach-Object { [regex]::Escape($_) }) -join '|'
  Assert-NoMatches `
    -Label 'RLS hardening SQL does not mutate app business rows' `
    -Path $HardeningSqlPath `
    -Patterns @(
      "^\s*insert\s+into\s+(public\.)?($tableAlternation)\b",
      "^\s*update\s+(public\.)?($tableAlternation)\b",
      "^\s*delete\s+from\s+(public\.)?($tableAlternation)\b",
      "^\s*truncate\s+(table\s+)?(public\.)?($tableAlternation)\b",
      "^\s*merge\s+into\s+(public\.)?($tableAlternation)\b",
      "^\s*copy\s+(public\.)?($tableAlternation)\b",
      "execute\s+[^`r`n]*(insert\s+into|update|delete\s+from|truncate|merge\s+into|copy)\s+(public\.)?($tableAlternation)\b"
    )
}

function Assert-HardeningSqlDmlIsReviewedStorageBucketMetadataOnly {
  param(
    [string]$HardeningSqlPath
  )

  $text = Get-Content -Raw $HardeningSqlPath
  $violations = @()
  $directDmlMatches = [regex]::Matches(
    $text,
    '(?im)^\s*(?<operation>insert\s+into|update|delete\s+from|truncate(?:\s+table)?|merge\s+into|copy)\s+(?<target>[a-zA-Z_][\w]*(?:\.[a-zA-Z_][\w]*)?)\b'
  )

  foreach ($match in $directDmlMatches) {
    $operation = ($match.Groups['operation'].Value -replace '\s+', ' ').ToLowerInvariant()
    $target = $match.Groups['target'].Value.ToLowerInvariant()
    $lineNumber = ($text.Substring(0, $match.Index) -split "`r?`n").Count

    if ($target -ne 'storage.buckets' -or ($operation -ne 'insert into' -and $operation -ne 'update')) {
      $violations += "${HardeningSqlPath}:$lineNumber unexpected hardening DML: $($match.Value.Trim())"
    }
  }

  if ($directDmlMatches.Count -ne 1) {
    $violations += "expected exactly one reviewed storage.buckets metadata upsert statement, found $($directDmlMatches.Count)"
  }

  if ($text -notmatch '(?is)insert\s+into\s+storage\.buckets\s*\(\s*id,\s*name,\s*public\s*\).*on\s+conflict\s*\(\s*id\s*\)\s+do\s+update\s+set\s+public\s*=\s*excluded\.public') {
    $violations += 'reviewed storage.buckets DML must be an id/name/public upsert that updates only the public flag'
  }

  $expectedBucketFlags = @{
    'bug-screenshots' = 'false'
    'fragebogen-response-images' = 'false'
    'gl-profile-pictures' = 'false'
    'vorbesteller-lieferung' = 'false'
    'wellen-photos' = 'false'
    'question-images' = 'true'
    'wellen-images' = 'true'
  }

  foreach ($bucketName in $expectedBucketFlags.Keys) {
    $expectedFlag = $expectedBucketFlags[$bucketName]
    $bucketPattern = "\('$([regex]::Escape($bucketName))'\s*,\s*'$([regex]::Escape($bucketName))'\s*,\s*$expectedFlag\)"
    if ($text -notmatch $bucketPattern) {
      $violations += "reviewed storage.buckets upsert missing expected flag for $bucketName=$expectedFlag"
    }
  }

  foreach ($match in [regex]::Matches($text, '(?im)\bexecute\s+[^`r`n]*(insert\s+into|update|delete\s+from|truncate|merge\s+into|copy)\b')) {
    $lineNumber = ($text.Substring(0, $match.Index) -split "`r?`n").Count
    $violations += "${HardeningSqlPath}:$lineNumber dynamic DML is not reviewed: $($match.Value.Trim())"
  }

  if ($violations.Count -gt 0) {
    Write-Host 'FAIL RLS hardening SQL DML is limited to reviewed Storage bucket metadata'
    $violations | ForEach-Object { Write-Host ("  " + $_) }
    throw 'RLS hardening SQL DML boundary failed'
  }

  Write-Host 'PASS RLS hardening SQL DML is limited to reviewed Storage bucket metadata'
}

function Assert-VerifierCheckCountDocumented {
  param(
    [string]$VerifierSqlPath,
    [string]$ReadmePath,
    [string]$ChecklistPath,
    [int]$ExpectedCheckCount
  )

  $verifierSql = Get-Content -Raw $VerifierSqlPath
  $checkNames = [regex]::Matches($verifierSql, "'([^']+)'\s+as\s+check_name") |
    ForEach-Object { $_.Groups[1].Value }
  $checkCount = $checkNames.Count

  if ($checkCount -le 0) {
    Write-Host 'FAIL metadata verifier has expected check names'
    throw 'metadata verifier check count failed'
  }

  if ($checkCount -ne $ExpectedCheckCount) {
    Write-Host 'FAIL metadata verifier preserves expected check count'
    Write-Host "  expected: $ExpectedCheckCount"
    Write-Host "  actual: $checkCount"
    throw 'metadata verifier check count changed'
  }

  Write-Host 'PASS metadata verifier preserves expected check count'

  $duplicateCheckNames = $checkNames | Group-Object | Where-Object { $_.Count -gt 1 }
  if ($duplicateCheckNames) {
    Write-Host 'FAIL metadata verifier has unique check names'
    $duplicateCheckNames | ForEach-Object { Write-Host ("  duplicate: " + $_.Name) }
    throw 'metadata verifier check names are duplicated'
  }

  Write-Host 'PASS metadata verifier has unique check names'

  Assert-FileContainsPatterns `
    -Label 'DSGVO docs document metadata verifier check count' `
    -Path $ReadmePath `
    -Patterns @(
      "currently emits $checkCount catalog-only checks"
    )

  Assert-FileContainsPatterns `
    -Label 'production checklist documents metadata verifier check count' `
    -Path $ChecklistPath `
    -Patterns @(
      "Confirm the verifier emits $checkCount checks"
    )
}

function Assert-SupabaseDependencyPinned {
  param(
    [string]$PackageJsonPath,
    [string]$PackageLockPath
  )

  $script = @'
const fs = require('fs');
const packageJson = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const packageLock = JSON.parse(fs.readFileSync(process.argv[3], 'utf8'));
const declaredVersion = packageJson.dependencies?.['@supabase/supabase-js'];
const lockedRootVersion = packageLock.packages?.['']?.dependencies?.['@supabase/supabase-js'];
const installedVersion = packageLock.packages?.['node_modules/@supabase/supabase-js']?.version;
if (!declaredVersion || !lockedRootVersion || !installedVersion) {
  throw new Error('missing Supabase dependency metadata');
}
if (/^[\^~><=*xX]/.test(declaredVersion) || /\|\||\s/.test(declaredVersion)) {
  throw new Error(`Supabase dependency uses a version range: ${declaredVersion}`);
}
if (declaredVersion !== lockedRootVersion || declaredVersion !== installedVersion) {
  throw new Error(`Supabase dependency mismatch package=${declaredVersion} rootLock=${lockedRootVersion} installed=${installedVersion}`);
}
'@

  $tempScript = Join-Path ([System.IO.Path]::GetTempPath()) ("dsgvo-supabase-pin-check-" + [System.Guid]::NewGuid().ToString("N") + ".js")
  try {
    Set-Content -Path $tempScript -Value $script -Encoding UTF8
    & node $tempScript $PackageJsonPath $PackageLockPath
    if ($LASTEXITCODE -ne 0) {
      throw 'Supabase dependency pin check failed'
    }
  } finally {
    Remove-Item -LiteralPath $tempScript -ErrorAction SilentlyContinue
  }

  Write-Host 'PASS backend Supabase dependency is pinned in package and lockfile'
}

Push-Location $backendRoot
try {
  Assert-EnvFilesIgnoredAndUntracked `
    -FrontendRoot $frontendRoot `
    -BackendRoot $backendRoot

  Assert-NoTrailingWhitespaceInFiles `
    -Label 'DSGVO package files have no trailing whitespace' `
    -Path @('scripts', 'sql')

  Assert-NoMojibakeInFiles `
    -Label 'backend source, SQL, and audit scripts have no mojibake' `
    -Path @('src/**/*.ts', 'sql/*.sql', 'sql/*.md', 'scripts/*.ps1')

  Assert-SupabaseDependencyPinned `
    -PackageJsonPath 'package.json' `
    -PackageLockPath 'package-lock.json'

  Assert-FileContainsPatterns `
    -Label 'backend rejects frontend or anon Supabase keys for service-role client' `
    -Path 'src/config/supabase.ts' `
    -Patterns @(
      'const validateServerOnlySupabaseKey = \(key: string\): void =>',
      "key\.startsWith\('sb_publishable_'\)",
      "key\.startsWith\('sb_secret_'\)",
      "jwtPayload\.role !== 'service_role'",
      'process\.env\.SUPABASE_SERVICE_KEY \|\| process\.env\.SUPABASE_SERVICE_ROLE_KEY',
      'validateServerOnlySupabaseKey\(resolvedSupabaseServiceKey\)'
    )

  Assert-PatternOrder `
    -Label 'auth router is mounted before global /api auth middleware' `
    -Path 'src/index.ts' `
    -Before "app\.use\('/api/auth'" `
    -After "app\.use\('/api', authenticateToken\)"

  Assert-AllBusinessApiRoutersAfterAuth -Path 'src/index.ts'

  Assert-RouteModulesMountedAndReviewed `
    -IndexPath 'src/index.ts' `
    -RoutesRoot 'src/routes'

  Assert-AdminRoutersMountedWithRequireAdmin -Path 'src/index.ts'

  Assert-GlPersonalDataRoutesAreScoped -RoutesRoot 'src/routes'

  Assert-AccountLifecycleInvalidatesRouteAuth `
    -RoutesRoot 'src/routes' `
    -MiddlewarePath 'src/middleware/auth.ts'

  Assert-HttpBoundaryIsHardened -IndexPath 'src/index.ts'

  Assert-AuthEndpointsAreAbuseHardened -AuthRoutePath 'src/routes/auth.ts'

  Assert-FileContainsPatterns `
    -Label 'Fragebogen route request logging sanitizes dynamic path ids' `
    -Path 'src/routes/fragebogen.ts' `
    -Patterns @(
      'const sanitizeLoggedPath = \(value: string\): string =>',
      'sanitizeLoggedPath\(req\.path\)'
    )

  Assert-NoMatches `
    -Label 'backend request logs do not print raw dynamic path ids' `
    -Path @('src/index.ts', 'src/routes/*.ts') `
    -Patterns @(
      'console\.(log|warn|error)\([^;\n]*\$\{req\.path\}',
      'console\.(log|warn|error)\([^;\n]*\+\s*req\.path'
    )

  Assert-NoMatches `
    -Label 'auth routes do not log raw auth or database errors' `
    -Path 'src/routes/auth.ts' `
    -Patterns @(
      'console\.error\([^;\n]*,\s*(error|err|authError|profileError|updateError|deleteError|insertError)\s*\)',
      'console\.warn\([^;\n]*,\s*(error|err|authError|profileError|updateError|deleteError|insertError)\s*\)',
      'console\.log\([^;\n]*,\s*(error|err|authError|profileError|updateError|deleteError|insertError)\s*\)'
    )

  Assert-NoMatches `
    -Label 'GL profile routes do not log raw auth, storage, or database errors' `
    -Path 'src/routes/gebietsleiter.ts' `
    -Patterns @(
      'console\.error\([^;\n]*,\s*(error|err|authError|profileError|updateError|deleteError|insertError|uploadError|userError|glError)\s*\)',
      'console\.warn\([^;\n]*,\s*(error|err|authError|profileError|updateError|deleteError|insertError|uploadError|userError|glError)\s*\)',
      'console\.log\([^;\n]*,\s*(error|err|authError|profileError|updateError|deleteError|insertError|uploadError|userError|glError)\s*\)'
    )

  Assert-NoMatches `
    -Label 'photo and Storage routes do not log raw upload/export errors' `
    -Path @('src/routes/fragebogen.ts', 'src/routes/wellen.ts', 'src/routes/bugReports.ts') `
    -Patterns @(
      'console\.(error|warn|log)\([^;\n]*(photo|Photo|image|Image|Storage|storage|ZIP|zip|screenshot|Screenshot|delivery|Delivery|upload|Upload|pending photos|Fotofragen)[^;\n]*,\s*(error|err|uploadError|archiveError|warning|photoError|updateError|dbError|downloadError|storageError)(\.|\)|\s|$)',
      'console\.(error|warn|log)\([^;\n]*(photo|Photo|image|Image|Storage|storage|ZIP|zip|screenshot|Screenshot|delivery|Delivery|upload|Upload|pending photos|Fotofragen)[^;\n]*,\s*(error|err|uploadError|archiveError|warning|photoError|updateError|dbError|downloadError|storageError)\.message'
    )

  Assert-NoMatches `
    -Label 'Fragebogen personal activity routes do not log raw errors' `
    -Path 'src/routes/fragebogen.ts' `
    -Patterns @(
      'console\.(error|warn|log)\([^;\n]*(response|Response|zeiterfassung|Zeiterfassung|day tracking|Day tracking|market visit|Market visit|market start|Market start|KM|km_stand|day summary|GL response history|completed response)[^;\n]*,\s*(error|err|responseError|updateError|deleteError|insertError|fetchError|zeitError)(\.|\)|\s|$)',
      'console\.(error|warn|log)\([^;\n]*(response|Response|zeiterfassung|Zeiterfassung|day tracking|Day tracking|market visit|Market visit|market start|Market start|KM|km_stand|day summary|GL response history|completed response)[^;\n]*,\s*(error|err|responseError|updateError|deleteError|insertError|fetchError|zeitError)\.message'
    )

  Assert-NoMatches `
    -Label 'sales and activity submission routes do not log raw errors' `
    -Path @('src/routes/wellen.ts', 'src/routes/vorverkauf.ts', 'src/routes/vorverkaufWellen.ts', 'src/routes/naraIncentive.ts', 'src/routes/activities.ts') `
    -Patterns @(
      'console\.(error|warn|log)\([^;\n]*(submission|Submission|progress|Progress|vorverkauf|Vorverkauf|vorbestell|Vorbestell|NARA|Incentive|activity|Activity|entry|Entry|fulfill|pending|batch|Delivery-photo|Palette fetch|Schuetten fetch|foto tags)[^;\n]*,\s*(error|err|updateError|deleteError|insertError|fetchError|subsError|progressError|vorverkaufError|naraError|productError|submissionError|entryError|ownershipError|palError|schError|tagError)(\.|\)|\s|$)',
      'console\.(error|warn|log)\([^;\n]*(submission|Submission|progress|Progress|vorverkauf|Vorverkauf|vorbestell|Vorbestell|NARA|Incentive|activity|Activity|entry|Entry|fulfill|pending|batch|Delivery-photo|Palette fetch|Schuetten fetch|foto tags)[^;\n]*,\s*(error|err|updateError|deleteError|insertError|fetchError|subsError|progressError|vorverkaufError|naraError|productError|submissionError|entryError|ownershipError|palError|schError|tagError)\.message'
    )

  Assert-WriteRoutesHaveReviewedAuthorization -RoutesRoot 'src/routes'

  Assert-AllRouteDefinitionsAreAuditable -RoutesRoot 'src/routes'

  Assert-ReadRoutesHaveReviewedAuthorization -RoutesRoot 'src/routes'

  Assert-NoMatches `
    -Label 'RLS SQL has no broad authenticated grants or deprecated privileged patterns' `
    -Path 'sql/*.sql' `
    -Patterns @(
      'grant select on table .*authenticated',
      'grant select, insert',
      'grant all on table .*authenticated',
      'auth\.role\('
    )

  Assert-NoRawRegexMatches `
    -Label 'RLS SQL has no public SECURITY DEFINER functions' `
    -Path 'sql/dsgvo_rls_hardening.sql' `
    -Patterns @(
      'create\s+or\s+replace\s+function\s+public\.[\s\S]*?security\s+definer',
      'create\s+function\s+public\.[\s\S]*?security\s+definer'
    )

  Assert-FileContainsPatterns `
    -Label 'RLS hardening SQL has production timeout guardrails' `
    -Path 'sql/dsgvo_rls_hardening.sql' `
    -Patterns @(
      '(?m)^begin;',
      "set local lock_timeout = '5s';",
      "set local statement_timeout = '5min';",
      'commit;'
    )

  Assert-HardeningSqlTransactionGuardrailsOrdered -Path 'sql/dsgvo_rls_hardening.sql'

  Assert-FileContainsPatterns `
    -Label 'RLS GL helper lives in private schema' `
    -Path 'sql/dsgvo_rls_hardening.sql' `
    -Patterns @(
      'create schema if not exists app_private',
      'create or replace function app_private\.app_gl_id_text\(\)',
      'security definer',
      "set search_path = ''",
      'where u\.id = \(select auth\.uid\(\)\)',
      'select app_private\.app_gl_id_text\(\)'
    )

  Assert-NoMatches `
    -Label 'RLS SQL does not use unsafe JWT user metadata authorization' `
    -Path 'sql/*.sql' `
    -Patterns @(
      'auth\.jwt\(',
      'user_metadata',
      'raw_user_meta_data'
    )

  Assert-HardeningSqlDoesNotMutateAppRows -HardeningSqlPath 'sql/dsgvo_rls_hardening.sql'

  Assert-HardeningSqlDmlIsReviewedStorageBucketMetadataOnly -HardeningSqlPath 'sql/dsgvo_rls_hardening.sql'

  Assert-FileContainsPatterns `
    -Label 'metadata verifier checks schema and private helper grants' `
    -Path 'sql/dsgvo_rls_verify_metadata.sql' `
    -Patterns @(
      'private_helper_schema_not_publicly_usable',
      'private_helper_schema_usage_required_roles_present',
      'private_helper_execute_grants_limited',
      'private_helper_execute_required_roles_present',
      'public_schema_create_not_publicly_granted',
      'public_schema_usage_not_anon_or_public',
      'public_schema_usage_required_roles_present',
      'postgres_public_default_privileges_hardened',
      'aclexplode\(coalesce\(n\.nspacl',
      'cfg\.setting in \(''search_path='', ''search_path=""''\)'
    )

  Assert-FileContainsPatterns `
    -Label 'RLS SQL closes public schema usage to anon and PUBLIC' `
    -Path 'sql/dsgvo_rls_hardening.sql' `
    -Patterns @(
      'revoke create on schema public from public',
      'revoke usage on schema public from public',
      'revoke usage on schema public from anon',
      'grant usage on schema public to authenticated, service_role'
    )

  Assert-FileContainsPatterns `
    -Label 'metadata verifier fails unreviewed public tables and views' `
    -Path 'sql/dsgvo_rls_verify_metadata.sql' `
    -Patterns @(
      'no_unreviewed_public_tables',
      'reviewed_public_tables\(table_name\)',
      "'spatial_ref_sys'",
      'no_unreviewed_public_views',
      'reviewed_public_views\(view_name\)',
      'pg_matviews',
      "'geography_columns'",
      "'geometry_columns'"
    )

  Assert-FileContainsPatterns `
    -Label 'metadata verifier checks service_role app table access' `
    -Path 'sql/dsgvo_rls_verify_metadata.sql' `
    -Patterns @(
      'service_role_app_table_access_present',
      'required_service_role_privileges\(privilege_type\)',
      "'IN' \|\| 'SERT'",
      "'UP' \|\| 'DATE'",
      "'DE' \|\| 'LETE'",
      "p\.grantee = 'service_role'",
      'missing_service_role_table_access'
    )

  Assert-FileContainsPatterns `
    -Label 'metadata verifier checks public sequence privileges' `
    -Path 'sql/dsgvo_rls_verify_metadata.sql' `
    -Patterns @(
      'no_direct_anon_authenticated_sequence_privileges',
      'public_sequence_privileges as',
      "c\.relkind = 'S'",
      "grantee in \('anon', 'authenticated'\)",
      'service_role_public_sequence_access_present',
      'required_service_role_sequence_privileges\(privilege_type\)',
      "'UP' \|\| 'DATE'",
      "grantee_role\.rolname = 'service_role'"
    )

  Assert-FileContainsPatterns `
    -Label 'metadata verifier checks service_role protected view and function access' `
    -Path 'sql/dsgvo_rls_verify_metadata.sql' `
    -Patterns @(
      'service_role_protected_view_access_present',
      'missing_view_access',
      "p\.grantee = 'service_role'",
      "p\.privilege_type = 'SELECT'",
      'service_role_protected_function_access_present',
      'missing_function_access',
      "p\.privilege_type = 'EXECUTE'"
    )

  Assert-FileContainsPatterns `
    -Label 'metadata verifier fails unreviewed public application functions' `
    -Path 'sql/dsgvo_rls_verify_metadata.sql' `
    -Patterns @(
      'no_unreviewed_public_application_functions',
      'reviewed_public_application_functions\(function_name\)',
      'public_application_functions as',
      "d\.classid = 'pg_proc'::regclass",
      "d\.deptype = 'e'",
      'pg_get_function_identity_arguments\(r\.oid\)',
      'ext\.oid is null'
    )

  Assert-FileContainsPatterns `
    -Label 'metadata verifier checks expected RLS policy shapes' `
    -Path 'sql/dsgvo_rls_verify_metadata.sql' `
    -Patterns @(
      'expected_policy_shapes_are_restricted',
      'expected_policy_shapes\(table_name, policyname, expected_cmd, allow_true_predicate\)',
      "roles @> array\['authenticated'\]::name\[\]",
      'cardinality\(roles\) = 1',
      'broad_using_predicate',
      'broad_with_check_predicate',
      'missing_using_or_with_check',
      "'IN' \|\| 'SERT'"
    )

  Assert-FileContainsPatterns `
    -Label 'RLS SQL hardens future public default privileges for postgres-owned objects' `
    -Path 'sql/dsgvo_rls_hardening.sql' `
    -Patterns @(
      'alter default privileges for role postgres in schema public revoke all on tables from anon, authenticated',
      'alter default privileges for role postgres in schema public revoke all on sequences from anon, authenticated',
      'alter default privileges for role postgres in schema public revoke execute on functions from public, anon, authenticated',
      'alter default privileges for role postgres in schema public grant all on tables to service_role',
      'alter default privileges for role postgres in schema public grant all on sequences to service_role',
      'alter default privileges for role postgres in schema public grant execute on functions to service_role'
    )

  Assert-NoMatches `
    -Label 'storage direct access is limited to reviewed public buckets' `
    -Path 'sql/dsgvo_rls_hardening.sql' `
    -Patterns @(
      'bucket_id\s+not\s+in'
    )

  Assert-FileContainsPatterns `
    -Label 'storage reviewed public buckets are explicit' `
    -Path 'sql/dsgvo_rls_hardening.sql' `
    -Patterns @(
      "storage\.buckets \(id, name, public\)",
      "\('question-images', 'question-images', true\)",
      "\('wellen-images', 'wellen-images', true\)",
      'storage\.objects is owned by Supabase'
    )

  Assert-FileContainsPatterns `
    -Label 'metadata verifier checks reviewed Storage buckets exist before flag checks' `
    -Path 'sql/dsgvo_rls_verify_metadata.sql' `
    -Patterns @(
      'expected_reviewed_storage_buckets_present',
      'expected_reviewed_storage_buckets\(bucket_id\)',
      'from storage\.buckets b',
      'where b\.id = e\.bucket_id',
      'sensitive_storage_bucket_public_flags'
    )

  Assert-StorageBucketsReferencedByCodeAreReviewed `
    -SourceRoot 'src' `
    -HardeningSqlPath 'sql/dsgvo_rls_hardening.sql' `
    -VerifierSqlPath 'sql/dsgvo_rls_verify_metadata.sql'

  Assert-SignedStorageUrlsAreShortLived -SourceRoot 'src'

  Assert-SignedStorageUrlsUseReviewedBuckets -SourceRoot 'src'

  Assert-PublicStorageUrlsUseReviewedPublicBuckets -SourceRoot 'src'

  Assert-StorageMutationsUseReviewedBuckets -SourceRoot 'src'

  Assert-RuntimeBucketConfigurationIsPrivateAndBounded -SourceRoot 'src'

  Assert-DocsCoverReviewedStorageBuckets `
    -ReadmePath 'sql/README_DSGVO_RLS.md' `
    -HardeningSqlPath 'sql/dsgvo_rls_hardening.sql' `
    -VerifierSqlPath 'sql/dsgvo_rls_verify_metadata.sql'

  Assert-NoMatches `
    -Label 'metadata verifier remains read-only' `
    -Path 'sql/dsgvo_rls_verify_metadata.sql' `
    -Patterns @(
      '\binsert\b',
      '\bupdate\b',
      '\bdelete\b',
      '\balter\b',
      '\bcreate\s+(schema|table|view|function|policy|trigger|index|extension)\b',
      '\bdrop\b',
      '\bgrant\b',
      '\brevoke\b'
    )

  Assert-MetadataSqlAvoidsBusinessDataSources `
    -Label 'metadata verifier does not read business tables or storage objects' `
    -Path 'sql/dsgvo_rls_verify_metadata.sql'

  Assert-MetadataSqlUsesReviewedSourcesOnly `
    -Label 'metadata verifier uses only reviewed catalog sources' `
    -Path 'sql/dsgvo_rls_verify_metadata.sql'

  Assert-FileContainsPatterns `
    -Label 'metadata verifier has runtime timeout guardrails' `
    -Path 'sql/dsgvo_rls_verify_metadata.sql' `
    -Patterns @(
      'set transaction read only;',
      "set local lock_timeout = '2s';",
      "set local statement_timeout = '2min';"
    )

  Assert-MetadataSqlReadOnlyTransactionGuardrailsOrdered `
    -Label 'metadata verifier read-only transaction guardrails are ordered' `
    -Path 'sql/dsgvo_rls_verify_metadata.sql'

  Assert-FileContainsPatterns `
    -Label 'preflight metadata snapshot covers rollback-critical metadata' `
    -Path 'sql/dsgvo_rls_preflight_metadata_snapshot.sql' `
    -Patterns @(
      "'table_privileges'",
      "'sequence_privileges'",
      "'schema_privileges'",
      "'rls_flags'",
      "'policies'",
      "'views'",
      "'functions'",
      "'storage_buckets'",
      "'bug-screenshots'",
      "'fragebogen-response-images'",
      "'gl-profile-pictures'",
      "'vorbesteller-lieferung'",
      "'wellen-photos'",
      "'question-images'",
      "'wellen-images'"
    )

  Assert-PreflightStorageBucketListMatchesHardeningAndVerifier `
    -HardeningSqlPath 'sql/dsgvo_rls_hardening.sql' `
    -VerifierSqlPath 'sql/dsgvo_rls_verify_metadata.sql' `
    -PreflightSqlPath 'sql/dsgvo_rls_preflight_metadata_snapshot.sql'

  Assert-NoMatches `
    -Label 'preflight metadata snapshot remains read-only' `
    -Path 'sql/dsgvo_rls_preflight_metadata_snapshot.sql' `
    -Patterns @(
      '\binsert\b',
      '\bupdate\b',
      '\bdelete\b',
      '\balter\b',
      '\bcreate\s+(schema|table|view|function|policy|trigger|index|extension)\b',
      '\bdrop\b',
      '\bgrant\b',
      '\brevoke\b'
    )

  Assert-MetadataSqlAvoidsBusinessDataSources `
    -Label 'preflight metadata snapshot does not read business tables or storage objects' `
    -Path 'sql/dsgvo_rls_preflight_metadata_snapshot.sql'

  Assert-MetadataSqlUsesReviewedSourcesOnly `
    -Label 'preflight metadata snapshot uses only reviewed catalog sources' `
    -Path 'sql/dsgvo_rls_preflight_metadata_snapshot.sql'

  Assert-FileContainsPatterns `
    -Label 'preflight metadata snapshot has runtime timeout guardrails' `
    -Path 'sql/dsgvo_rls_preflight_metadata_snapshot.sql' `
    -Patterns @(
      'set transaction read only;',
      "set local lock_timeout = '2s';",
      "set local statement_timeout = '2min';"
    )

  Assert-MetadataSqlReadOnlyTransactionGuardrailsOrdered `
    -Label 'preflight metadata snapshot read-only transaction guardrails are ordered' `
    -Path 'sql/dsgvo_rls_preflight_metadata_snapshot.sql'

  Assert-FileContainsPatterns `
    -Label 'DSGVO README documents preflight metadata snapshot' `
    -Path 'sql/README_DSGVO_RLS.md' `
    -Patterns @(
      'dsgvo_rls_preflight_metadata_snapshot\.sql',
      'It reads catalog metadata only, not business rows',
      'runs inside a read-only transaction with short local runtime timeouts',
      'preflight and verifier scripts run in read-only transactions and set local `lock_timeout` and `statement_timeout`',
      'save its output'
    )

  Assert-FileContainsPatterns `
    -Label 'production apply checklist documents safe DSGVO sequence' `
    -Path 'sql/DSGVO_PRODUCTION_APPLY_CHECKLIST.md' `
    -Patterns @(
      'Do not run business-data smoke tests or production data queries',
      'A fresh production backup exists',
      'SUPABASE_SERVICE_KEY` or the legacy/common alias `SUPABASE_SERVICE_ROLE_KEY`',
      'The deployed backend revision contains the route-auth, signed-URL, private bucket, generic error, and service-key validation changes',
      'The frontend production environment has no Supabase URL, anon, publishable, secret, or service-role key',
      'npm run dsgvo:audit',
      'using only normal health endpoints',
      'Run `dsgvo_rls_preflight_metadata_snapshot\.sql`',
      'metadata-only and runs in a read-only transaction with bounded local runtime timeouts',
      'Save the full preflight output with the backup/change record',
      'Apply `dsgvo_rls_hardening\.sql` as one transaction',
      'sets `lock_timeout` and `statement_timeout`',
      'if it times out, stop and inspect/reschedule',
      'Run `dsgvo_rls_verify_metadata\.sql`',
      "status = 'pass'",
      'Keep the preflight output, verifier output, backend deployment id, frontend deployment id, and backup id together in the change record',
      'A copy of `DSGVO_PRODUCTION_EVIDENCE_TEMPLATE\.md` is ready for the change record',
      'Complete the copied evidence template without adding business rows, personal data, photo paths, tokens, or service keys',
      'Prefer restoring the production backup taken for this change',
      'Backend service-role access still works, but route-level auth remains the live authorization boundary'
    )

  Assert-FileContainsPatterns `
    -Label 'production evidence template protects change record data boundaries' `
    -Path 'sql/DSGVO_PRODUCTION_EVIDENCE_TEMPLATE.md' `
    -Patterns @(
      'Do not paste business rows, user records, photo paths, tokens, service keys, or other personal data',
      'Backend `SUPABASE_SERVICE_KEY` / `SUPABASE_SERVICE_ROLE_KEY` server-only checked',
      'Production backup id',
      '`npm run dsgvo:audit` result',
      'Script: `dsgvo_rls_preflight_metadata_snapshot\.sql`',
      'Script: `dsgvo_rls_hardening\.sql`',
      'Script: `dsgvo_rls_verify_metadata\.sql`',
      'Expected result: exactly 28 checks',
      'No production business-data smoke tests run'
    )

  Assert-FileContainsPatterns `
    -Label 'DSGVO handoff indexes hardening package and boundaries' `
    -Path 'sql/DSGVO_HARDENING_HANDOFF.md' `
    -Patterns @(
      'dsgvo_rls_hardening\.sql',
      'dsgvo_rls_preflight_metadata_snapshot\.sql',
      'dsgvo_rls_verify_metadata\.sql',
      'DSGVO_PRODUCTION_EVIDENCE_TEMPLATE\.md',
      'read-only transaction with bounded local timeouts',
      'currently emits 28 checks',
      'npm run dsgvo:audit',
      'bounded dynamic Supabase `\.from\(table\)` helper calls',
      'Do not query production business rows for smoke tests',
      'Keep GL market visibility unchanged through the Express API/UI',
      'service-role access bypasses RLS',
      'Still Not Complete Until',
      'The production backup exists',
      'The matching backend/frontend deployment is live',
      'The preflight snapshot is saved',
      'The hardening SQL is applied',
      'The metadata verifier output proves all 28 checks pass'
    )

  Assert-FileContainsPatterns `
    -Label 'DSGVO README links production apply checklist' `
    -Path 'sql/README_DSGVO_RLS.md' `
    -Patterns @(
      'DSGVO_PRODUCTION_APPLY_CHECKLIST\.md',
      'DSGVO_PRODUCTION_EVIDENCE_TEMPLATE\.md',
      'record backup id, deployment ids, preflight output location, apply result, and verifier result',
      'short operator checklist',
      'short lock timeout and bounded statement timeout'
    )

  Assert-FileContainsPatterns `
    -Label 'DSGVO README documents dynamic Supabase helper boundary' `
    -Path 'sql/README_DSGVO_RLS.md' `
    -Patterns @(
      'Dynamic Supabase `\.from\(table\)` helper calls are separately bounded',
      'requires every caller of `requireOwnedRowOrAdmin`, `fetchRowsByIdsInChunks`, `fetchValueMap`, `fetchRowsByIdChunks`, and `fetchAdminRowsByIdChunks` to pass literal table names',
      'prevents future user-controlled or computed table names'
    )

  Assert-VerifierCheckCountDocumented `
    -VerifierSqlPath 'sql/dsgvo_rls_verify_metadata.sql' `
    -ReadmePath 'sql/README_DSGVO_RLS.md' `
    -ChecklistPath 'sql/DSGVO_PRODUCTION_APPLY_CHECKLIST.md' `
    -ExpectedCheckCount $expectedMetadataVerifierCheckCount

  Assert-FileContainsPatterns `
    -Label 'DSGVO README records current Supabase API exposure guidance' `
    -Path 'sql/README_DSGVO_RLS.md' `
    -Patterns @(
      'Current Supabase changelog review for this hardening pass: checked on 2026-06-30',
      '2026-04-28 - Tables not exposed to Data and GraphQL API automatically',
      'explicit-grant/RLS model',
      'No recent changelog item requires keeping direct `anon` or `authenticated` table grants open'
    )

  Assert-NoMatches `
    -Label 'metadata verifier uses deterministic pass/fail statuses' `
    -Path 'sql/dsgvo_rls_verify_metadata.sql' `
    -Patterns @(
      "'review'"
    )

  Assert-NoMatches `
    -Label 'GL profile pictures do not use public storage URLs' `
    -Path 'src/routes/gebietsleiter.ts' `
    -Patterns @(
      "from\('wellen-images'\)",
      'getPublicUrl'
    )

  Assert-NoMatches `
    -Label 'Wellen evidence photos do not upload to the public wave image bucket' `
    -Path 'src/routes/wellen.ts' `
    -Patterns @(
      '\.from\(WELLEN_IMAGES_BUCKET\)\.upload'
    )

  Assert-FileContainsPatterns `
    -Label 'image upload endpoints enforce explicit byte limits' `
    -Path 'src/routes/bugReports.ts' `
    -Patterns @(
      'BUG_SCREENSHOT_MAX_BYTES',
      'buffer\.length > BUG_SCREENSHOT_MAX_BYTES'
    )

  Assert-FileContainsPatterns `
    -Label 'Fragebogen image uploads enforce explicit byte limits' `
    -Path 'src/routes/fragebogen.ts' `
    -Patterns @(
      'FRAGEBOGEN_IMAGE_MAX_BYTES',
      'buffer\.length > FRAGEBOGEN_IMAGE_MAX_BYTES'
    )

  Assert-FileContainsPatterns `
    -Label 'Wellen image uploads enforce explicit byte limits' `
    -Path 'src/routes/wellen.ts' `
    -Patterns @(
      'WELLEN_PHOTO_MAX_BYTES',
      'DELIVERY_PHOTO_MAX_BYTES',
      'buffer\.length > WELLEN_PHOTO_MAX_BYTES',
      'buffer\.length > DELIVERY_PHOTO_MAX_BYTES'
    )

  Assert-NoMatches `
    -Label 'exports do not include raw private storage paths' `
    -Path 'src/utils/exportTransformers.ts' `
    -Patterns @(
      'delivery_photo_url:\s*sub\.delivery_photo_url',
      'profile_picture_url:\s*gl\.profile_picture_url',
      'photo_url:\s*sub\.photo_url'
    )

  Assert-NoMatches `
    -Label 'backend 500 responses do not expose internal error messages' `
    -Path @('src/routes/*.ts', 'src/middleware/*.ts') `
    -Patterns @(
      'res\.status\(500\)\.json\(\{\s*error:\s*error\.message',
      'res\.status\(500\)\.json\(\{\s*error:\s*err\.message',
      'res\.status\(500\)\.json\(\{\s*error:\s*e\.message',
      'res\.status\(500\)\.json\(\{\s*error:\s*error\s*\}',
      'res\.status\(500\)\.json\(\{\s*error:\s*err\s*\}',
      'res\.status\(500\)\.json\(\{\s*error:\s*e\s*\}'
    )

  Assert-NoMatches `
    -Label 'backend logs do not print request objects, credentials, or personal-data fields' `
    -Path @('src/routes/*.ts', 'src/middleware/*.ts', 'src/config/*.ts', 'src/utils/*.ts') `
    -Patterns @(
      'console\.(log|warn|error)\([^;\n]*(req\.body|req\.query|req\.params|req\.headers|req\.user)',
      'console\.(log|warn|error)\([^;\n]*(password|token|Authorization|authorization|cookie)',
      'console\.(log|warn|error)\([^;\n]*(email|firstName|lastName|gebietsleiter_name|profile_picture_url|screenshot_url|photo_url|user_agent)',
      'console\.(log|warn|error)\([^;\n]*,\s*(error|err|[A-Za-z]+Error)\b',
      'console\.(log|warn|error)\([^;\n]*(error|err|[A-Za-z]+Error)\.message',
      'console\.log\([^;\n]*payload',
      'console\.log\([^;\n]*summary'
    )

  Assert-NoMatches `
    -Label 'backend Supabase reads use explicit column lists' `
    -Path @('src/routes/*.ts', 'src/middleware/*.ts', 'src/utils/*.ts') `
    -Patterns @(
      "\.select\(\s*'\*'",
      '\.select\(\s*"\*"',
      '\.select\(\s*`\*`'
    )

  Assert-SupabaseObjectCoverage `
    -SourceRoot 'src' `
    -SqlPath 'sql/dsgvo_rls_hardening.sql'

  Assert-DynamicSupabaseFromCallsAreBounded -SourceRoot 'src'

  Assert-LocalSchemaObjectsCovered `
    -SchemaRoot $frontendRoot `
    -SqlPath 'sql/dsgvo_rls_hardening.sql'

  Assert-RlsPolicyColumnAssumptionsCovered -SchemaRoot $frontendRoot

  Assert-MarketVisitSourceValuesCovered `
    -RoutesRoot 'src/routes' `
    -SchemaRoot $frontendRoot

  Assert-RlsVerifierCoversHardeningTables `
    -HardeningSqlPath 'sql/dsgvo_rls_hardening.sql' `
    -VerifierSqlPath 'sql/dsgvo_rls_verify_metadata.sql'

  Assert-RlsVerifierCoversHardeningViewsFunctionsAndBuckets `
    -HardeningSqlPath 'sql/dsgvo_rls_hardening.sql' `
    -VerifierSqlPath 'sql/dsgvo_rls_verify_metadata.sql'

  Assert-RlsVerifierCoversHardeningPolicies `
    -HardeningSqlPath 'sql/dsgvo_rls_hardening.sql' `
    -VerifierSqlPath 'sql/dsgvo_rls_verify_metadata.sql'

  Assert-RlsVerifierPolicyShapeListMatchesExpectedPolicies `
    -VerifierSqlPath 'sql/dsgvo_rls_verify_metadata.sql'

  Assert-NoSqlCredentialSeeds -SchemaRoot $frontendRoot

  Assert-NoLegacyBroadRlsInLocalSchemaFiles -SchemaRoot $frontendRoot

  if (-not $SkipBuild) {
    Invoke-CheckedNpm -Arguments @('run', 'build')
  }
}
finally {
  Pop-Location
}

Push-Location $frontendRoot
try {
  $frontendAuditFiles = Get-FrontendSourceFiles -Root $frontendRoot
  if (($frontendAuditFiles | Measure-Object).Count -lt 3) {
    throw 'Frontend audit file discovery found too few files'
  }

  Assert-NoMojibakeInFiles `
    -Label 'frontend source and setup docs have no mojibake' `
    -Path ($frontendAuditFiles + @('AUTH_SETUP_GUIDE.md', 'AUTH_SETUP_SIMPLE.md', 'FRONTEND_ENV_SETUP.md'))

  Assert-NoMatches `
    -Label 'frontend has no Supabase client or service key exposure' `
    -Path $frontendAuditFiles `
    -Patterns @(
      '@supabase/supabase-js',
      'createClient',
      'VITE_SUPABASE',
      'SUPABASE_SERVICE',
      'SUPABASE_URL',
      'service_role',
      'service-role'
    )

  Assert-NoMatches `
    -Label 'frontend has no direct Supabase REST/Auth/Storage calls' `
    -Path $frontendAuditFiles `
    -Patterns @(
      'https://[^''"`\s]+\.supabase\.co',
      '\.supabase\.co/(rest|auth|storage)/v1',
      '/rest/v1',
      '/auth/v1',
      '/storage/v1',
      '[''"]apikey[''"]\s*:'
    )

  Assert-FileContainsPatterns `
    -Label 'frontend env docs keep Supabase keys server-only' `
    -Path 'FRONTEND_ENV_SETUP.md' `
    -Patterns @(
      'The frontend talks only to the MarsPets\+ backend API',
      'VITE_API_URL=http://localhost:3001/api',
      'Do not put Supabase URL, anon keys, or service-role keys in the frontend environment'
    )

  Assert-NoMatches `
    -Label 'frontend env docs do not provide browser Supabase variables' `
    -Path 'FRONTEND_ENV_SETUP.md' `
    -Patterns @(
      '(?m)^\s*VITE_SUPABASE',
      '(?m)^\s*SUPABASE_',
      '@supabase/supabase-js',
      'createClient'
    )

  Assert-FileContainsPatterns `
    -Label 'auth setup guide documents backend-only Supabase service key' `
    -Path 'AUTH_SETUP_GUIDE.md' `
    -Patterns @(
      'Frontend calls the backend API through `VITE_API_URL`',
      'Frontend does not use a Supabase client, anon key, or service-role key',
      'Backend stores `SUPABASE_SERVICE_KEY` or the legacy/common alias `SUPABASE_SERVICE_ROLE_KEY` only in `backend/.env` / production environment variables',
      'Do not add `VITE_SUPABASE_URL`, `VITE_SUPABASE_ANON_KEY`, or any service-role key to the frontend environment'
    )

  Assert-FileContainsPatterns `
    -Label 'simple auth setup documents frontend API-only env' `
    -Path 'AUTH_SETUP_SIMPLE.md' `
    -Patterns @(
      'The app now uses the backend API as the only browser-facing data/auth boundary',
      'Create a root `.env` with only the API URL',
      'Do not configure Supabase URL/anon/service keys in the frontend'
    )

  Assert-AuthenticatedFetchInstalledBeforeApp -Path (Join-Path $frontendRoot 'src/main.tsx')

  $fetchBypassAuditFiles = $frontendAuditFiles | Where-Object {
    $_ -ne (Join-Path $frontendRoot 'src/services/apiFetch.ts')
  }

  Assert-NoMatches `
    -Label 'frontend does not bypass authenticated fetch wrapper' `
    -Path $fetchBypassAuditFiles `
    -Patterns @(
      'window\.fetch\s*=',
      'originalFetch'
    )

  Assert-NoMatches `
    -Label 'frontend uses fetch wrapper instead of alternate browser transports' `
    -Path $fetchBypassAuditFiles `
    -Patterns @(
      '\bXMLHttpRequest\b',
      '\baxios\b',
      '\bnavigator\.sendBeacon\b',
      '\bglobalThis\.fetch\b'
    )

  Assert-NoMatches `
    -Label 'frontend API calls do not use relative /api URLs that bypass API_BASE_URL matching' `
    -Path $fetchBypassAuditFiles `
    -Patterns @(
      'fetch\(\s*[''"]/api',
      'fetch\(\s*`/api'
    )

  if (-not $SkipBuild) {
    Invoke-CheckedNpm -Arguments @('run', 'build')
  }
}
finally {
  Pop-Location
}

Write-Host 'PASS DSGVO local audit complete'
