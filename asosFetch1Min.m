%%asosFetch1Min
%   Fetch ASOS 1-minute surface observations for a specific date range.
%
%   Unlike ASOSdownloadFiveMin (which pulls entire monthly .dat files from
%   NCEI's now-retired FTP archive), asosFetch1Min queries the Iowa
%   Environmental Mesonet (IEM) ASOS 1-minute service and returns ONLY the
%   requested time window. No local .dat files, no month-at-a-time
%   downloads, no FTP, no email argument.
%
%   General form:
%       T = asosFetch1Min(station, startTime, endTime)
%       T = asosFetch1Min(station, startTime, endTime, Name, Value, ...)
%
%   Inputs:
%       station    Four-character ICAO identifier (e.g. 'KORD', 'KJFK') as
%                  a char or string, OR a cell/string array of identifiers
%                  for multiple stations.  Leading 'K' is stripped
%                  automatically for US airports (IEM's convention).
%                  Non-K prefixes (e.g. 'PANC' for Anchorage) are passed
%                  through.
%       startTime  MATLAB datetime (interpreted as UTC if naive).
%       endTime    MATLAB datetime (must be > startTime).
%
%   Optional Name-Value pairs:
%       'Variables'   Cell/string array of IEM variable names. Default:
%                     {'tmpf','dwpf','sknt','drct','gust_sknt',
%                      'vis1_coeff','vis1_nd','pres1','precip'}
%                     tmpf       Air temperature (degF)
%                     dwpf       Dewpoint (degF)
%                     sknt       Wind speed (knots, 2-min average)
%                     drct       Wind direction (deg, 2-min average)
%                     gust_sknt  Peak wind gust in last minute (knots)
%                     vis1_coeff Primary visibility sensor extinction coef
%                     vis1_nd    Primary visibility sensor night/day flag
%                     pres1      Station pressure (inches Hg)
%                     precip     1-minute precipitation accumulation (in)
%       'Timezone'    IANA tz string (default 'UTC'). Controls the tz
%                     parameter sent to IEM; the returned 'valid' column
%                     is always a MATLAB datetime tagged with that zone.
%       'SaveCsvTo'   If given, the raw CSV is also written to this path
%                     for provenance/reproducibility.
%       'Timeout'     Seconds before giving up on the HTTP request
%                     (default 120).
%       'Verbose'     true/false (default false). If true, prints the
%                     request URL before fetching.
%
%   Output:
%       T   MATLAB table. Columns: station, station_name, valid (datetime,
%           UTC) plus whichever variables were requested. Rows are sorted
%           by time then station. Missing values are NaN.
%
%   Example 1 - a single storm interval at ORD:
%       t0 = datetime(2024,1,15,12,0,0, 'TimeZone','UTC');
%       t1 = datetime(2024,1,15,15,0,0, 'TimeZone','UTC');
%       T  = asosFetch1Min('KORD', t0, t1);
%       plot(T.valid, T.tmpf); ylabel('T (degF)');
%
%   Example 2 - three NYC-area stations, temperature only:
%       t0 = datetime(2018,12,9,9,0,0,  'TimeZone','UTC');
%       t1 = datetime(2018,12,9,22,0,0, 'TimeZone','UTC');
%       T  = asosFetch1Min({'KJFK','KLGA','KEWR'}, t0, t1, ...
%                          'Variables', {'tmpf','dwpf'});
%
%   Data source:
%       Iowa Environmental Mesonet ASOS 1-minute service.
%       https://mesonet.agron.iastate.edu/request/asos/1min.phtml
%       IEM ingests the official NCEI 1-minute ASOS page-1 / page-2
%       archives under
%       https://www.ncei.noaa.gov/data/automated-surface-observing-system-one-minute-pg1/
%       and exposes them as a queryable CSV endpoint.
%
%   Requires: MATLAB R2017a+ (uses webread, websave, readtable, datetime).
%
%   Written: 2026-04-14
%   See also ASOSimportFiveMin, surfacePlotter, stormFinder.

function T = asosFetch1Min(station, startTime, endTime, varargin)

% -------- input validation ------------------------------------------------
if nargin < 3
    error('asosFetch1Min:NotEnoughInputs', ...
          'Usage: T = asosFetch1Min(station, startTime, endTime, ...)');
end
if ~(isa(startTime, 'datetime') && isa(endTime, 'datetime'))
    error('asosFetch1Min:BadTimes', ...
          'startTime and endTime must be MATLAB datetimes.');
end
if endTime <= startTime
    error('asosFetch1Min:BadRange', 'endTime must be after startTime.');
end

% Assume UTC if caller supplied naive datetimes; otherwise convert.
if isempty(startTime.TimeZone)
    startTime.TimeZone = 'UTC';
end
if isempty(endTime.TimeZone)
    endTime.TimeZone = 'UTC';
end
startUTC = datetime(startTime, 'TimeZone', 'UTC');
endUTC   = datetime(endTime,   'TimeZone', 'UTC');

% -------- name-value parsing (inputParser; avoids the R2019b arguments
% block so this function still runs on R2017a, per the repo's README) -----
defaultVars = {'tmpf','dwpf','sknt','drct','gust_sknt', ...
               'vis1_coeff','vis1_nd','pres1','precip'};
p = inputParser;
p.FunctionName = 'asosFetch1Min';
addParameter(p, 'Variables', defaultVars, @(x) iscell(x) || isstring(x));
addParameter(p, 'Timezone',  'UTC',       @(x) ischar(x) || isstring(x));
addParameter(p, 'SaveCsvTo', '',          @(x) ischar(x) || isstring(x));
addParameter(p, 'Timeout',   120,         @(x) isnumeric(x) && x > 0);
addParameter(p, 'Verbose',   false,       @(x) islogical(x) || isnumeric(x));
parse(p, varargin{:});
opts = p.Results;

vars = cellstr(opts.Variables);
tz   = char(opts.Timezone);

% -------- normalize station list ------------------------------------------
% IEM's ASOS 1-min service wants the bare 3-letter FAA ID for US stations
% (ORD not KORD), but full 4-letter codes for non-US (PANC stays PANC).
stationsIn = cellstr(station);
stationsIEM = cell(size(stationsIn));
for k = 1:numel(stationsIn)
    s = upper(strtrim(stationsIn{k}));
    if strlength(s) == 4 && s(1) == 'K'
        stationsIEM{k} = s(2:end);   % strip leading K for US ASOS
    else
        stationsIEM{k} = s;          % pass-through (e.g. PANC, PHNL)
    end
end

% -------- build request ---------------------------------------------------
url = 'https://mesonet.agron.iastate.edu/cgi-bin/request/asos1min.py';
params = { ...
    'station',  strjoin(stationsIEM, ','), ...
    'vars',     strjoin(vars, ','), ...
    'sts',      localIsoUtc(startUTC), ...
    'ets',      localIsoUtc(endUTC), ...
    'sample',   '1min', ...
    'what',     'download', ...
    'delim',    'comma', ...
    'tz',       tz };

if opts.Verbose
    fprintf('asosFetch1Min: requesting %s?%s\n', url, localQs(params));
end

% -------- fetch to a temp file (readtable handles CSV robustly) -----------
tmpFile = [tempname '.csv'];
webOpts = weboptions('Timeout', opts.Timeout, 'ContentType', 'text');
cleanup = onCleanup(@() localTryDelete(tmpFile)); %#ok<NASGU>
websave(tmpFile, url, params{:}, webOpts);

% IEM returns HTTP 200 with an error message in the body for bad input.
% Detect that by peeking at the first line BEFORE trying to parse as CSV.
fid = fopen(tmpFile, 'r');
firstLine = fgetl(fid);
fclose(fid);
if ischar(firstLine) && ~startsWith(firstLine, 'station,')
    error('asosFetch1Min:RequestRejected', ...
          'IEM rejected the request: %s', firstLine);
end

% -------- parse -----------------------------------------------------------
readOpts = detectImportOptions(tmpFile, 'NumHeaderLines', 0);
% Force the valid(UTC) column to be read as text so we can parse datetimes
% ourselves (readtable's auto-inference sometimes misfires on the parens).
validCol = 'valid_UTC_';  % MATLAB sanitizes 'valid(UTC)' to this name
if ismember(validCol, readOpts.VariableNames)
    readOpts = setvartype(readOpts, validCol, 'char');
end
T = readtable(tmpFile, readOpts);

if isempty(T)
    warning('asosFetch1Min:EmptyResult', ...
            'No rows returned for the requested station/date range.');
    return;
end

% -------- post-process ----------------------------------------------------
% 1. Rename 'valid_UTC_' -> 'valid' and convert to datetime
if ismember(validCol, T.Properties.VariableNames)
    T.valid = datetime(T.(validCol), ...
                       'InputFormat', 'yyyy-MM-dd HH:mm', ...
                       'TimeZone', 'UTC');
    T.(validCol) = [];
    % Put 'valid' right after 'station_name' for readability.
    % (Manual reorder instead of movevars, which is R2018a+.)
    cols = T.Properties.VariableNames;
    lead = intersect({'station','station_name','valid'}, cols, 'stable');
    rest = setdiff(cols, lead, 'stable');
    T = T(:, [lead, rest]);
end

% 2. Sort by time then station
if ismember('valid', T.Properties.VariableNames)
    sortKeys = {'valid'};
    if ismember('station', T.Properties.VariableNames)
        sortKeys{end+1} = 'station'; %#ok<AGROW>
    end
    T = sortrows(T, sortKeys);
end

% 3. Save CSV for provenance if requested
if ~isempty(char(opts.SaveCsvTo))
    copyfile(tmpFile, char(opts.SaveCsvTo));
end

end

% ==========================================================================
% Helpers
% ==========================================================================

function s = localIsoUtc(dt)
% Format a datetime as IEM's expected ISO 8601 UTC string (yyyy-MM-ddTHH:mmZ).
    s = char(datetime(dt, 'TimeZone', 'UTC', ...
                      'Format', 'yyyy-MM-dd''T''HH:mm''Z'''));
end

function qs = localQs(params)
% Rebuild the query string for verbose logging (not used for the real call).
    bits = cell(1, numel(params)/2);
    for i = 1:2:numel(params)
        bits{(i+1)/2} = sprintf('%s=%s', params{i}, params{i+1});
    end
    qs = strjoin(bits, '&');
end

function localTryDelete(path)
    if exist(path, 'file')
        try
            delete(path);
        catch
            % nothing to do; OS will clean tempdir eventually
        end
    end
end
