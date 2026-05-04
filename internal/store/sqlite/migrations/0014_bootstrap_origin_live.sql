UPDATE venues
SET origin = 'live'
WHERE origin = 'seed'
  AND slug IN (
    'cafe-no-9',
    'corporation',
    'leadmill',
    'lescar',
    'greystones',
    'yellow-arch',
    'sidney-and-matilda'
  );

UPDATE events
SET origin = 'live'
WHERE origin = 'seed'
  AND slug IN (
    'matinee-noise-at-the-leadmill',
    'neepsend-afterhours',
    'courtyard-wildcards',
    'leadmill-late-room'
  );
