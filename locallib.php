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

class cli_interface {

    protected $help = null;
    protected $version = null;

    function cli_progress_bar($progress, $total, $info="", $width=50) {
        global $tput_cols;
        $perc = round(($progress * 100) / $total);
        $bar = round(($width * $perc) / 100);
        $pb = sprintf("%s%%[%s>%s]", $perc, str_repeat("=", $bar), str_repeat(" ", $width-$bar));
        $info_p = str_pad($info, $tput_cols-strlen($pb)-4, ' ');
        return "$pb$info_p\r";
    }
    
    function get_help() {
        if (empty($this->help)) {
            return "No help to you.";
        }
        $help = $this->help;
        
        if ($this->get_option_args() == 0) {
            return $help;
        }

        $help .= "\nOptions:\n";
        $len = 0;
        foreach ($this->get_option_args() as $option => $configs) {
            $pading_option = $option . (array_key_exists('value', $configs) ? "={$configs['value']}" : '');
            $len = strlen($pading_option) > $len ? strlen($pading_option) : $len;
        }
        foreach ($this->get_option_args() as $option => $configs) {
            $pading_option = str_pad($option . (array_key_exists('value', $configs) ? "={$configs['value']}" : ''), $len, " ");
            $option_help = array_key_exists('help', $configs) ? $configs['help'] : 'No help to you today.';
            $choices = array_key_exists('choices', $configs) ? 'Choices are: '. implode(', ', $configs['choices']) . '.' : '';
            $default = array_key_exists('default', $configs) && !empty($configs['default']) ? "Default are {$configs['default']}." : '';
            $help .= "   --$pading_option $option_help $choices$default\n";
        }
        $help .= '';
        return  $help;
    }

    function get_version() {
        return $this->version ? $this->version : 'Moodle version: ' . get_config('', 'version') . "\n";
    }

    public function get_option_args() {
        return [
            "help"=>['label'=>['databasetypehead', 'install'], 'help'=> 'Print this page and exit.'],
            "version"=>['label'=>['databasetypehead', 'install'], 'help'=> 'Print version number and exit.'],
        ];
    }

    function read_options() {
        $options_to_read = [];
        foreach ($this->get_option_args() as $option => $configs) {
            $options_to_read[$option] = array_key_exists('default', $configs) ? $configs['default'] : null;
        }
        list($options, $unrecognized) = cli_get_params($options_to_read, []);

        if ($options['help']) {
            echo $this->get_help();
            exit(0);
        }

        if ($options['version']) {
            echo $this->get_version();
            exit(0);
        }

        foreach ($this->get_option_args() as $option => $configs) {
            if ( !isset($options[$option]) && array_key_exists('value', $configs) && !empty($configs['value'])) {
                $label = array_key_exists('label', $configs) && !empty($configs['label']) ? get_string($configs['label'][0], $configs['label'][1]) : "Type $option";
                $choices = array_key_exists('choices', $configs) && !empty($configs['choices']) ? $configs['choices'] : null;
                $default = array_key_exists('default', $configs) && !empty($configs['default']) ? $configs['default'] : null;
                $label_to_print = !empty($choices) ? "$label. Choices: " . implode($choices) : $label; 
                $options[$option] = cli_input($label_to_print, $default, $choices, !empty($choices));
            }
        }
        $this->options=$options;
    }
}
