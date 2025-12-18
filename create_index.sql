-- Script SQL pour créer un index réduit sur la table `observations`
-- Collez ce script dans l'onglet SQL de phpMyAdmin (base: maladies_db) et exécutez.

ALTER TABLE `observations`
  ADD INDEX `idx_obs_mia` (`maladie`(100), `indicateur`(100), `annee`);

-- Vérifier les index créés :
-- SHOW INDEX FROM `observations`;

-- Si besoin de supprimer l'index :
-- ALTER TABLE `observations` DROP INDEX `idx_obs_mia`;
