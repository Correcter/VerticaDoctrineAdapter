<?php

/**
 * Форматирует строку в виде TSV и записывает её в файловый указатель
 * @param resource $handle Указатель (resource) на файл, обычно создаваемый с помощью функции fopen().
 * @param array $fields
 * @return int
 */
if(!function_exists('fputtsv')){
    function fputtsv($handle, array $fields)
    {
        return fwrite($handle, implode("\t", $fields) . "\n");
    }
}