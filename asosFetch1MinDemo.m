%%asosFetch1MinDemo
%   Short demo for asosFetch1Min. Pulls 3 hours of 1-minute data at KORD
%   around a 2024 winter storm and makes two basic plots.
%
%   Run with:  asosFetch1MinDemo

% 1. Pick a date range (UTC).
t0 = datetime(2024, 1, 15, 12, 0, 0, 'TimeZone', 'UTC');
t1 = datetime(2024, 1, 15, 15, 0, 0, 'TimeZone', 'UTC');

% 2. Fetch. Returns a table with columns:
%    station, station_name, valid (datetime, UTC), tmpf, dwpf, sknt,
%    drct, gust_sknt, vis1_coeff, vis1_nd, pres1, precip
T = asosFetch1Min('KORD', t0, t1, 'Verbose', true);

fprintf('Fetched %d rows, %d columns.\n', height(T), width(T));
disp(head(T));

% 3. Temperature + dewpoint timeseries.
figure;
plot(T.valid, T.tmpf, '-', 'DisplayName', 'Temperature');
hold on;
plot(T.valid, T.dwpf, '-', 'DisplayName', 'Dewpoint');
ylabel('\circF');
title(sprintf('%s  %s - %s UTC', T.station{1}, ...
              datestr(t0,'yyyy-mm-dd HH:MM'), datestr(t1,'HH:MM')));
legend('Location', 'best');
grid on;

% 4. Wind speed + gust.
figure;
plot(T.valid, T.sknt, '-', 'DisplayName', 'Wind (2-min avg)');
hold on;
plot(T.valid, T.gust_sknt, '-', 'DisplayName', 'Gust (1-min peak)');
ylabel('knots');
title(sprintf('Wind at %s', T.station{1}));
legend('Location', 'best');
grid on;
