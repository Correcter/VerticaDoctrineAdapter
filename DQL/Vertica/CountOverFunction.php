<?php

namespace VerticaDoctrineAdapter\DQL\Vertica;

use Doctrine\ORM\Query\Lexer;
use Doctrine\ORM\Query\AST\Functions\FunctionNode;

/**
 * Кастомная агрегатная функция COUNT() OVER() для вертики
 * 
 * Функция не поддерживает передачу аргументов, надо бы расширить
 * 
 * Вызывается как countOwer()
 *
 * @author Vitaly Dergunov (<correcter@inbox.ru>)
 */
class CountOverFunction extends FunctionNode
{

    /**
     * @var \Doctrine\ORM\Query\AST\ArithmeticExpression
     */
    private $field;

    /**
     * @param \Doctrine\ORM\Query\Parser $parser
     */
    public function parse(\Doctrine\ORM\Query\Parser $parser)
    {
        $parser->match(Lexer::T_IDENTIFIER);
        $parser->match(Lexer::T_OPEN_PARENTHESIS);
        $this->field = $parser->ArithmeticExpression(); 
        $parser->match(Lexer::T_CLOSE_PARENTHESIS);
    }

    public function getSql(\Doctrine\ORM\Query\SqlWalker $sqlWalker)
    {
        return sprintf(
                "COUNT(%s) OVER()", 
                $sqlWalker->walkArithmeticFactor($this->field)
        );
    }
}
