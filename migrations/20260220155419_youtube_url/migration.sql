/*
  Warnings:

  - Added the required column `youtubeUrl` to the `songs` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE `songs` ADD COLUMN `youtubeUrl` TEXT NOT NULL;
