\copy mutations_foncieres (id_mutation, date_mutation, numero_disposition, nature_mutation, valeur_fonciere, adresse_numero, adresse_suffixe, adresse_nom_voie, adresse_code_voie, code_postal, code_commune, nom_commune, code_departement, ancien_code_commune, ancien_nom_commune, id_parcelle, ancien_id_parcelle, numero_volume, lot1_numero, lot1_surface_carrez, lot2_numero, lot2_surface_carrez, lot3_numero, lot3_surface_carrez, lot4_numero, lot4_surface_carrez, lot5_numero, lot5_surface_carrez, nombre_lots, code_type_local, type_local, surface_reelle_bati, nombre_pieces_principales, code_nature_culture, nature_culture, code_nature_culture_speciale, nature_culture_speciale, surface_terrain, longitude, latitude) FROM '/data/full_171819.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', ENCODING 'UTF8', NULL '');

\echo 'Data loading from full_171819.csv completed.'

VACUUM ANALYZE mutations_foncieres;
\echo 'VACUUM ANALYZE completed.'