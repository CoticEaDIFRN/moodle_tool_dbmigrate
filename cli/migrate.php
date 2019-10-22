<?php
// This file is part of Moodle - http://moodle.org/
//
// Moodle is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Moodle is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with Moodle.  If not, see <http://www.gnu.org/licenses/>.

/**
 * This script migrates data from current database to another
 *
 * This script is not intended for beginners!
 * Potential problems:
 * - su to apache account or sudo before execution
 * - already broken DB scheme or invalid data
 *
 * @package    tool_dbmigrate
 * @copyright  2012 Petr Skoda {@link http://skodak.org/}
 * @license    http://www.gnu.org/copyleft/gpl.html GNU GPL v3 or later
 */

define('CLI_SCRIPT', true);

require(__DIR__.'/../../../../config.php');
require_once($CFG->libdir.'/clilib.php');
require_once(__DIR__.'/../locallib.php');

$help = "Database migration script.

It is strongly recommended to turn off the web server
or enable CLI maintenance mode before starting the migration.

Options:
--dbtype=TYPE         Database type.
--dblibrary=TYPE      Database library. Defaults to 'native'.
--dbhost=HOST         Database host.
--dbname=NAME         Database name.
--dbuser=USERNAME     Database user.
--dbpass=PASSWORD     Database password.
--dbport=NUMBER       Database port.
--prefix=STRING       Table prefix for above database tables.
--dbsocket=PATH       Use database sockets. Available for some databases only.
-h, --help            Print out this help.

Example:
\$ sudo -u www-data /usr/bin/php admin/tool/dbmigrate/cli/migrate.php\n";
$tput_cols = intval(exec('tput cols'));

function cli_progress_bar($progress, $total, $info="", $width=50) {
    global $tput_cols;
    $perc = round(($progress * 100) / $total);
    $bar = round(($width * $perc) / 100);
    $pb = sprintf("%s%%[%s>%s]", $perc, str_repeat("=", $bar), str_repeat(" ", $width-$bar));
    $info_p = str_pad($info, $tput_cols-strlen($pb)-4, ' ');
    return "$pb$info_p\r";
}

class dbmigrate_migrate {
    protected $targetdb = null;
    protected $tables_metas = null;
    protected $tables_on_target = [];
    protected $table_count = 0;
    protected $total_rows = 0;
    protected $options = null;
    protected $page_size = 1000;
    protected $exclude = ['course_modules_completion', 'files', 'analytics_indicator_calc', 'logstore_standard_log', 
                          'game_cross', 'game_cryptex', 'game_hangman', 'game_millionaire', 
                          'grade_grades', 'notifications', 'message_read', 
                          'question_attempt_step_data', 'question_attempt_steps', 'stats_daily'];
    protected $only_tables = null;
    // protected $only_tables = ['block_positions'];
    protected $only_first = false;

    function get_options() {
        global $CFG, $DB, $help;

        // Now get cli options.
        list($options, $unrecognized) = cli_get_params(
            array(
                'dbtype'            => null,
                'dblibrary'         => 'native',
                'dbhost'            => null,
                'dbport'            => null,
                'dbname'            => null,
                'dbuser'            => null,
                'dbpass'            => null,
                'prefix'            => null,
                'dbsocket'          => null,
                'maintenance'       => null,
                'list'              => false,
                'help'              => false,
                'force'             => false,
            ),
            array(
                'm' => 'maintenance',
                'l' => 'list',
                'h' => 'help',
            )
        );
    
        if ($options['help']) {
            echo $help;
            exit(0);
        }
    
        if (empty($CFG->version)) {
            cli_error(get_string('missingconfigversion', 'debug'));
        }
    
        $drivers = tool_dbmigrate_get_drivers();
    
        if (!isset($options['dbtype'])) {
            $choose = array();
            foreach ($drivers as $driver => $name) {
                list($dbtype, $dblibrary) = explode('/', $driver);
                $choose[$dbtype] = $dbtype;
            }
            $optionsstr = implode(', ', $choose);
            cli_heading(get_string('databasetypehead', 'install')." ($optionsstr)");
            $options['dbtype'] = cli_input(get_string('clitypevalue', 'admin'), '', $choose, true);
        }
    
        $choose = array();
        foreach ($drivers as $driver => $name) {
            list($dbtype, $dblibrary) = explode('/', $driver);
            if ($dbtype === $options['dbtype']) {
                $choose[$dblibrary] = $dblibrary;
            }
        }
        if (!isset($options['dblibrary']) or !isset($choose[$options['dblibrary']])) {
            $optionsstr = implode(', ', $choose);
            cli_heading('Database library'." ($optionsstr)"); // Note: no need to localise unless we add real PDO drivers.
            $options['dblibrary'] = cli_input(get_string('clitypevalue', 'admin'), '', $choose, true);
        }
    
        if (!isset($options['dbhost'])) {
            cli_heading(get_string('databasehost', 'install'));
            $options['dbhost'] = cli_input(get_string('clitypevalue', 'admin'));
        }
    
        if (!isset($options['dbport'])) {
            cli_heading(get_string('dbport', 'install'));
            $options['dbport'] = cli_input(get_string('clitypevalue', 'admin'));
        }
    
        if (!isset($options['dbname'])) {
            cli_heading(get_string('databasename', 'install'));
            $options['dbname'] = cli_input(get_string('clitypevalue', 'admin'));
        }
    
        if (!isset($options['dbuser'])) {
            cli_heading(get_string('databaseuser', 'install'));
            $options['dbuser'] = cli_input(get_string('clitypevalue', 'admin'));
        }
    
        if (!isset($options['dbpass'])) {
            cli_heading(get_string('databasepass', 'install'));
            $options['dbpass'] = cli_input(get_string('clitypevalue', 'admin'));
        }
    
        if (!isset($options['prefix'])) {
            cli_heading(get_string('dbprefix', 'install'));
            $options['prefix'] = cli_input(get_string('clitypevalue', 'admin'));
        }
    
        if ($CFG->ostype !== 'WINDOWS') {
            if (!isset($options['dbsocket'])) {
                cli_heading(get_string('databasesocket', 'install'));
                $options['dbsocket'] = cli_input(get_string('clitypevalue', 'admin'));
            }
        }
        $this->options = $options;
    }

    function check_connection() {
        // Try target DB connection.    
        $this->targetdb = moodle_database::get_driver_instance($this->options['dbtype'], $this->options['dblibrary']);
        $options = $this->options;
        $dboptions = array();
        if ($this->options['dbport']) {
            $dboptions['dbport'] = $this->options['dbport'];
        }
        if ($options['dbsocket']) {
            $dboptions['dbsocket'] = $this->options['dbsocket'];
        }
        try {
            $this->targetdb->connect($options['dbhost'], $options['dbuser'], $options['dbpass'], $options['dbname'], $options['prefix'], $dboptions);
            $this->target_tables = $this->targetdb->get_tables();
        } catch (moodle_exception $e) {
            $problem = $e->debuginfo."\n\n";
            $problem .= get_string('notargetconectexception', 'tool_dbmigrate');
            echo "PROBLEM: $problem\n\n";
            exit(1);
        }
    }
        
    function get_tables_indexes($full_table_name) {
        global $DB, $CFG;
        $indexes = [];
        $last = '';
        $rows = $DB->get_records_sql("SELECT   concat(t.constraint_name, ' (', s.seq_in_index, ')') pk, 
                                               t.constraint_name, t.constraint_type, s.column_name, s.seq_in_index
                                      FROM     INFORMATION_SCHEMA.STATISTICS s
                                           INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
                                            ON (t.TABLE_SCHEMA = s.TABLE_SCHEMA
                                                AND t.TABLE_NAME = s.TABLE_NAME
                                                AND t.CONSTRAINT_NAME = s.INDEX_NAME)
                                      WHERE    s.table_schema = ?
                                      AND      s.table_name = ?
                                      ORDER BY t.constraint_name, seq_in_index",
                                     [$CFG->dbname, $full_table_name]);
        foreach ($rows as $row) {
            if ($last != $row->constraint_name) {
                $indexes[$row->constraint_name] = ['type'=>$row->constraint_type, 'columns'=>[]];
            }
            array_push($indexes[$row->constraint_name]['columns'], $row->column_name);
            $last = $row->constraint_name;
        }
        return $indexes;
    }

    function get_tables_metas() {
        global $DB, $CFG;
        if (is_null($this->tables_metas)) {
            $this->tables_metas = [];
            $this->table_count = count($DB->get_tables());
            echo "LENDO O MODELO DAS TABELAS\n";
    
            $i = 0;
            $tables = $this->only_tables ? $this->only_tables : $DB->get_tables();
            foreach ($tables as $table_name) {
                $i++;
                if ( in_array($table_name, $this->exclude)) {
                    continue;
                } 
                echo "  " . cli_progress_bar($i, $this->table_count, " $i/{$this->table_count}");
                
                $full_table_name = $DB->get_prefix() . $table_name;
                $count_records = $DB->count_records($table_name);

                $this->tables_metas[$table_name] = [
                    'row_count'=> $count_records, 
                    'columns'=> $DB->get_records_sql("SELECT column_name, c.* FROM information_schema.columns c WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position", [$CFG->dbname, $full_table_name]),
                    'full_table_name'=>$full_table_name,
                    'indexes'=>$this->get_tables_indexes($full_table_name),
                ];
                
                $this->total_rows += $count_records;

                if ('0' != $this->targetdb->get_field_sql("SELECT count(1) qtd FROM information_schema.tables t WHERE table_name = ?", [$full_table_name])) {
                    $this->tables_on_target[] = $table_name;
                }
                if ($this->only_first) {return $this->tables_metas;}
            }
            echo "\n  Lidos $i modelos.\n\n";
            $this->table_count = count($this->tables_metas);
        }
        return $this->tables_metas;
    }

    function print_summary() {
        echo "\n\nSUMÁRIO\n";
        echo "  Total de tabelas a migrar: " . count($this->tables_metas) . "\n";
        echo "  Total de tabelas já existentes: " . count($this->tables_on_target) . "\n";
        echo "  Total de tabelas a criar: " . (count($this->tables_metas) - count($this->tables_on_target)) . "\n";
        echo "  Total de linhas a migrar : " . number_format($this->total_rows, 0, "", ".") . "\n\n";
        if ( (count($this->tables_on_target)>0) && (!$this->options['force']) )  {
            $continue = cli_input('  Desejas continuar? (Y,y,N,n)', '', ['Y', 'y', 'N', 'n', ], true);
            if (strtoupper($continue) != 'Y') { 
                die('  Migração cancelada.');
            }    
        }
    }
    
    function create_tables() { 
        $i = 0;
        echo "CRIANDO AS TABELAS\n";
        foreach ($this->get_tables_metas() as $table_name => $table_meta) {
            $i++;
            echo "  " . cli_progress_bar($i, $this->table_count, " {$table_name} ($i de $this->table_count)");

            if (in_array($table_name, $this->tables_on_target)) {
                $this->targetdb->execute("TRUNCATE TABLE {$table_meta['full_table_name']}");
            } else {
                $table_struc = "";
                foreach ($table_meta['columns'] as $column) {
                    if ($table_struc != "") {
                        $table_struc .= ",\n";
                    }
                    $table_struc .= "  {$column->column_name} ";
                    switch (strtolower($column->data_type)) {
                        case 'varchar':
                        case 'decimal':
                            $table_struc .= $column->column_type;
                            break;
                        case 'float':
                            $table_struc .= "numeric({$column->numeric_precision}, {$column->numeric_scale})";
                            break;
                        case 'double':
                            $table_struc .= 'double precision';
                            break;
                        case 'longtext':
                            $table_struc .= 'text';
                            break;
                        case 'tinyint':
                        case 'smallint':
                            $table_struc .= 'smallint';
                            break;
                        case 'mediumint':
                        case 'int':
                            $table_struc .= 'integer';
                            break;
                        case 'bigint':
                            $table_struc .= $column->extra == 'auto_increment' ? 'bigserial' : $column->data_type;
                            break;
                        default:
                            die('Formato não tratatdo.');
                            break;
                    }
                    $table_struc .= $column->is_nullable == 'YES' ? " NULL" : " NOT NULL";
                    if (!empty($column->column_default)) {
                        $table_struc .= " DEFAULT ";
                        switch (strtolower($column->data_type)) {
                            case 'varchar':
                            case 'longtext':
                                $table_struc .= "'{$column->column_default}'";
                                break;
                            default:
                                $table_struc .= $column->column_default;
                                break;
                        }
                    }
                }
                $sql = "CREATE TABLE {$table_meta['full_table_name']} (\n$table_struc\n)";
                $this->targetdb->execute($sql);

                foreach ($table_meta['indexes'] as $index_name => $index_meta) {
                    $columns = implode(', ', $index_meta['columns']);
                    if ($index_meta['type'] == 'PRIMARY KEY') {
                        $this->targetdb->execute("ALTER TABLE {$table_meta['full_table_name']} ADD PRIMARY KEY ($columns)");
                    } else {
                        $this->targetdb->execute("CREATE UNIQUE INDEX $index_name ON {$table_meta['full_table_name']} ($columns)");
                    }
                }
            }
        }
        echo "  " . cli_progress_bar($this->table_count, $this->table_count, " Criadas ($i de $this->table_count).") . "\n";
    }

    function insert_rows() {
        global $DB, $tput_cols;
        $t = 0;
        $tt = count($this->get_tables_metas());
        echo "\nINSERINDO AS LINHAS\n";
        foreach ($this->get_tables_metas() as $table_name => $table_meta) {
            $t++;
            $p = round(($t * 100) / $tt);
            $row_count_f = number_format($table_meta['row_count'], 0, "", ".");
            for ($j=0; $j<=$table_meta['row_count']; $j+=$this->page_size) { 
                echo "    " . cli_progress_bar($j, $table_meta['row_count'], " $table_name ($p% - $t de $tt). Linhas ($migrated_in_table_f de $row_count_f)");
                $migrated_in_table_f = number_format($j, 0, "", ".");
                $rs = $DB->get_recordset($table_name, null, '', '*', $j+1, $this->page_size);
                $this->targetdb->insert_records($table_name, $rs);
                $rs->close();
            }
        }
        echo "  " . cli_progress_bar($tt, $tt, " Inseridas ($t de $tt)."). "\n";
    }
    
    function reset_sequences() {
        $i = 0;
        $sql = "";
        echo "\nREDEFININDO AS SEQUENCES\n";
        $sql = "SELECT column_default, column_name, table_name
        FROM information_schema.columns c
        WHERE table_catalog = ?
        AND table_schema = 'public'
        AND table_name like ?
        AND column_default like 'nextval%'";
        $params = [$this->options['dbname'], $this->options['prefix'] . '%'];
        $rows = $this->targetdb->get_records_sql($sql, $params);
        $tt = count($rows);
        foreach ($rows as $column) {
            $i++;
            $sequence_name = substr($column->column_default, 9, -12);
            $this->targetdb->execute("SELECT setval('$sequence_name', (select MAX({$column->column_name})+1 qtd from {$column->table_name}))");
            echo "    " . cli_progress_bar($i, $tt, " $sequence_name ($i de $tt)");
        }
        echo "  " . cli_progress_bar($i, $tt, " Redefinidas ($i de $tt)."). "\n";
    }

    function migrate() {
        global $DB;
        $this->get_options();
        $this->check_connection();
        $this->get_tables_metas();
        $this->print_summary();
        $this->create_tables();
        $this->insert_rows();
        // $this->reset_sequences();
        echo "\n";
    }
}

$dbmigrate_migrate = new dbmigrate_migrate();
$dbmigrate_migrate->migrate();
