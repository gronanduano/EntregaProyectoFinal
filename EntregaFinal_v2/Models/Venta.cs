namespace Ventas.Models
{
    public class Venta
    {
        public int IdUsuario { get; set; }
        public int IdProducto { get; set; }
        public string Descripciones { get; set; }
        public int Stock { get; set; }
        public double PrecioVenta { get; set; }
        public double Valor_Venta { get; set; }
    }

}
