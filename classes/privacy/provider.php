<?php
/**
 * Privacy Subsystem implementation for tool_dbmigrate.
 *
 * @package    tool_dbmigrate
 * @copyright  2019 Kelson Medeiros {@link https://github.com/kelsoncm}
 * @license    https://opensource.org/licenses/MIT The MIT License
 */
namespace tool_dbmigrate\privacy;

defined('MOODLE_INTERNAL') || die();

class provider implements \core_privacy\local\metadata\null_provider {

    public static function get_reason() : string {
        return 'privacy:metadata';
    }
}