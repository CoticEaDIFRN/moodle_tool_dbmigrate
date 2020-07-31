<?php
/**
 * This script migrates data from current database to another
 *
 * @package    tool_dbmigrate
 * @copyright  2019 Kelson Medeiros {@link https://github.com/kelsoncm}
 * @license    https://opensource.org/licenses/MIT The MIT License
 */

define('CLI_SCRIPT', true);

require(__DIR__.'/../../../../config.php');
require_once($CFG->libdir.'/clilib.php');
require_once(__DIR__.'/../locallib.php');

$tput_cols = intval(exec('tput cols'));

ini_set('memory_limit', '1G');

class dbmigrate_migrate extends cli_interface {
    protected $targetdb = null;
    protected $tables_metas = null;
    protected $tables_on_target = [];
    protected $table_count = 0;
    protected $total_rows = 0;
    protected $options = null;
    protected $page_size = 1;

    protected $help = "New Moodle database migration script.\n\nIt is strongly recommended to turn off the web server\n or enable CLI maintenance mode before starting the migration.\n\nExample:\n\$ sudo -u www-data /usr/bin/php admin/tool/dbmigrate/cli/migrate.php\n\n";
    
    public function get_option_args() {
        return array_merge(
            [
                "prefix"=>['value'=>'PREFIX', 'label'=>"Type tables prefix", 'default'=>'mdl_', 'help'=> 'Table prefix for above database tables.'],
                "dbconnector"=>['value'=> 'CONNECTOR', 'default'=>null, 'choices'=> $this->get_drivers(), 'help'=> 'Database type/library.'],
                "dbhost"=>['value'=> 'HOST', 'help'=> 'Database host.'],
                "dbport"=>['value'=> 'PORT', 'help'=> 'Database port.'],
                "dbname"=>['value'=> 'DATABASE', 'help'=> 'Database name.'],
                "dbuser"=>['value'=> 'USERNAME', 'help'=> 'Database user.'],
                "dbpass"=>['value'=> 'PASSWORD', 'help'=> 'Database password.'],
                "only_tables"=>['help'=> 'Restrict migration to this tables.'],
                "exclude"=>['help'=> 'Migrate all table, exception this tables.'],
                "page_size"=>['help'=> 'Number of lines to bulk insert.'],
                "maintenance"=>['help'=> 'Put Moodle on maintenance during migration.'],
                "force"=>['help'=> 'Force migration if tables already exists.'],
            ], 
            parent::get_option_args());
    }

    private function get_drivers() {
        $drives = tool_dbmigrate_get_drivers();
        $choose = array();
        foreach ($drives as $driver => $name) {
            // list($dbtype, $dblibrary) = explode('/', $driver);
            array_push($choose, $driver);
        }

        return $choose;
    }

    function check_connection() {
        list($dbtype, $dblibrary) = explode('/', $this->options['dbconnector']);

        $this->targetdb = moodle_database::get_driver_instance($dbtype, $dblibrary);
        $options = $this->options;
        $dboptions = array();
        if ($this->options['dbport']) {
            $dboptions['dbport'] = $this->options['dbport'];
        }
        if ($options['dbsocket']) {
            $dboptions['dbsocket'] = $this->options['dbsocket'];
        }
        try {
            $this->page_size = !empty($this->options['page_size']) ? intval($this->options['page_size']) : 1000;
            // $dboptions['bulkinsertsize'] = $this->page_size;
            $dboptions['bulkinsertsize'] = 1000;
            $this->targetdb->connect($options['dbhost'], $options['dbuser'], $options['dbpass'], $options['dbname'], $options['prefix'], $dboptions);
            $this->target_tables = $this->targetdb->get_tables();
            echo "Connection successful.\n\n";
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
            echo "LENDO O MODELO DAS TABELAS\n";
    
            $i = 0;
            $only_tables = !empty($this->options['only_tables']) ? explode(',', $this->options['only_tables']) : $DB->get_tables();
            $exclude = !empty($this->options['exclude']) ? explode(',', $this->options['exclude']) : [];
            $tables = array_diff($only_tables,$exclude);
            $this->table_count = count($tables);
            foreach ($tables as $table_name) {
                $i++;
                echo "  " . $this->cli_progress_bar($i, $this->table_count, " $i/$this->table_count");
                
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
        global $DB;
        echo "\n\nSUMÁRIO\n";
        echo "  Total de tabelas no Moodle: " . count($DB->get_tables()) . "\n";
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
            echo "  " . $this->cli_progress_bar($i, $this->table_count, " {$table_name} ($i de $this->table_count)");

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
                                if (substr($column->column_default, 0, 1) == "'") {
                                    $table_struc .= $column->column_default;
                                } else {
                                    $table_struc .= "'{$column->column_default}'";
                                }
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
        echo "  " . $this->cli_progress_bar($this->table_count, $this->table_count, " Criadas ($i de $this->table_count).") . "\n";
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
                echo "    " . $this->cli_progress_bar($j, $table_meta['row_count'], " $table_name ($p% - $t de $tt). Linhas ($migrated_in_table_f de $row_count_f)");
                $migrated_in_table_f = number_format($j, 0, "", ".");

                $rs = $DB->get_recordset($table_name, null, 'id', '*', $j+1, $this->page_size);
                // $rs = $DB->get_recordset($table_name, null, 'id', '*');
                $this->targetdb->insert_records($table_name, $rs);
                $rs->close();
            }
        }
        echo "  " . $this->cli_progress_bar($tt, $tt, " Inseridas ($t de $tt)."). "\n";
    }
    
    function reset_sequences() {
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
        $i = 1;
        foreach ($rows as $column) {
            if (array_key_exists(substr($column->table_name, strlen($this->options['prefix'])), $this->get_tables_metas())) {
                $i++;
                $sequence_name = substr($column->column_default, 9, -12);
                $this->targetdb->execute("SELECT setval('$sequence_name', (select MAX({$column->column_name})+1 qtd from {$column->table_name}))");
                echo "    " . $this->cli_progress_bar($i, $this->table_count, " $sequence_name ($i de $this->table_count)");
            }
        }
        echo "  " . $this->cli_progress_bar($i, $this->table_count, " Redefinidas ($i de $this->table_count)."). "\n";
    }

    function execute() {
        try {
            $this->read_options();
            $this->check_connection();
            $this->get_tables_metas();
            $this->print_summary();
            $this->create_tables();
            $this->insert_rows();
            $this->reset_sequences();
            echo "\n";
        } catch (Exception $e) {
            print_r($e);
        }
    }
}

$dbmigrate_migrate = new dbmigrate_migrate();
$dbmigrate_migrate->execute();
