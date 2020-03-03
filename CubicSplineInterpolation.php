<?php
#error_reporting(1);
/**
 * Created by PhpStorm.
 * User: Dejan
 * Date: 17.7.2017
 * Time: 13:03
 */

# **CUBIC SPLINE INTERPOLATION**
# > https://www.value-at-risk.net/cubic-spline-interpolation/ <

#echo $_SERVER["DOCUMENT_ROOT"]."spline/numphp/vendor/autoload.php";

define('__ROOT__', dirname(__FILE__));
require_once(__ROOT__.'\numphp\vendor\autoload.php');

use NumPHP\Core\NumArray;
use NumPHP\LinAlg\LinAlg;


function spline($x,$y,$rez) {
    $ls=count($x)-1;
    $M=array();
    $B=array();
    $s=4*$ls;
    $z=array_fill(0, 4*$ls, 0);
    $k=array();

    for ($i=0;$i<$ls;$i++) {
        $idx_y=2*$i;
        $M[$idx_y]=array_merge(array_slice($z, 0, 4*$i), array($x[$i]**3, $x[$i]**2, $x[$i], 1), array_slice($z, 0, $s-4*$i-4)); # Pi(xi)
        $B[$idx_y]=$y[$i];
        $M[$idx_y+1]=array_merge(array_slice($z, 0, 4*$i), array($x[$i+1]**3, $x[$i+1]**2, $x[$i+1], 1), array_slice($z, 0, $s-4*$i-4)); # Pi(xi+1)
        $B[$idx_y+1]=$y[$i+1];
    }
    $idx_y+=2;

    for ($i=1; $i<$ls; $i++) {
        $M[$idx_y]=array_merge(array_slice($z, 0, 4*$i-4), array(3*$x[$i]**2, 2*$x[$i], 1, 0, -3*$x[$i]**2, -2*$x[$i], -1, 0), array_slice($z, 0, $s-4*$i-4)); #P'i=P'i+1
        $B[$idx_y]=0;
        $idx_y++;
        $M[$idx_y]=array_merge(array_slice($z, 0, 4*$i-4), array(6*$x[$i], 2, 0, 0, -6*$x[$i], -2, 0, 0), array_slice($z, 0, $s-4*$i-4)); #P''i=P''i+1
        $B[$idx_y]=0;
        $idx_y++;
    }

    $M[$idx_y]=array_pad(array(6*$x[0], 2),$s, 0);
    $B[$idx_y]=0;
    $M[$idx_y+1]=array_pad(array(6*end($x), 2,0,0),-$s, 0);
    $B[$idx_y+1]=0;

    # [M] * [k] = [B]  ==> [k] = [M]**(-1) * [B]

    $M=new NumArray($M);
    $M=LinAlg::inv($M);
    $k=$M->dot($B);
    $k=$k->get('0:')->data;
    $xosa=array();
    $y_out=array();
    $x_out=array();

    for ($i=0;$i<$ls;$i++) {
        $x_out=array_merge($x_out, array_slice(range($x[$i], $x[$i+1], ($x[$i+1]-$x[$i])/$rez),0, $rez)); #
        for ($j=0; $j<$rez; $j++) {
            $y_out[]=$k[0+$i*4]*$x_out[$j+$i*$rez]**3+$k[1+$i*4]*$x_out[$j+$i*$rez]**2+$k[2+$i*4]*$x_out[$j+$i*$rez]+$k[3+$i*4];
        }
    }
    return array($x_out, $y_out);
}

$out=spline(array(1, 2, 3, 4), array(1, 7, 2, 5), 30);
