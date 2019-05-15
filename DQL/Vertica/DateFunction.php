<?php

namespace VerticaDoctrineAdapter\DQL\Vertica;

use Doctrine\ORM\Query\Lexer;
use Doctrine\ORM\Query\AST\Functions\FunctionNode;

/**
 * Description of DateFunction
 *
 * @author Vitaly Dergunov (<correcter@inbox.ru>)
 */
class DateFunction extends FunctionNode
{
    /*
     * Holds the date of the DATE statement
     * @var mixed
     */
    protected $dateExpression;

    /**
     * getSql
     *
     * @param \Doctrine\ORM\Query\SqlWalker $sqlWalker
     * @access public
     * @return string
     */
    public function getSql(\Doctrine\ORM\Query\SqlWalker $sqlWalker)
    {
        return 'DATE(' . $this->dateExpression->dispatch($sqlWalker) . ')';
    }

    /**
     * parse
     *
     * @param \Doctrine\ORM\Query\Parser $parser
     * @access public
     * @return void
     */
    public function parse(\Doctrine\ORM\Query\Parser $parser)
    {
        $parser->match(Lexer::T_IDENTIFIER);
        $parser->match(Lexer::T_OPEN_PARENTHESIS);
        $this->dateExpression = $parser->StringPrimary();
        $parser->match(Lexer::T_CLOSE_PARENTHESIS);
    }
}
