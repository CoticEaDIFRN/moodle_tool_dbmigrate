<?php
/**
 * Migrate database
 *
 * @package    tool_dbmigrate
 * @copyright  2019 Kelson Medeiros {@link https://github.com/kelsoncm}
 * @license    https://opensource.org/licenses/MIT The MIT License
 */

defined('MOODLE_INTERNAL') || die;

require_once($CFG->libdir.'/adminlib.php');
require_once($CFG->libdir.'/dtllib.php');

/**
 * Returns list of fully working database drivers present in system.
 * @return array
 */
function tool_dbmigrate_get_drivers() {
    global $CFG;

    $files = new RegexIterator(new DirectoryIterator("$CFG->libdir/dml"), '|^.*_moodle_database\.php$|');
    $drivers = array();

    foreach ($files as $file) {
        $matches = null;
        preg_match('|^([a-z0-9]+)_([a-z]+)_moodle_database\.php$|', $file->getFilename(), $matches);
        if (!$matches) {
            continue;
        }
        $dbtype = $matches[1];
        $dblibrary = $matches[2];

        if ($dbtype === 'sqlite3') {
            // Blacklist unfinished drivers.
            continue;
        }

        $targetdb = moodle_database::get_driver_instance($dbtype, $dblibrary, false);
        if ($targetdb->driver_installed() !== true) {
            continue;
        }

        $driver = $dbtype.'/'.$dblibrary;

        $drivers[$driver] = $targetdb->get_name();
    };
    return ['pgsql/native'=>'PostgreSQL (native/pgsql)'];
    return $drivers;
}
