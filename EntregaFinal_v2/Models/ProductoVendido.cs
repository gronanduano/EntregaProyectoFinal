namespace ProductosVendidos.Models
{
    public class ProductoVendido
    {
        public int IdUsuario { get; set; }
        public int IdProducto { get; set; }
        public string Descripciones { get; set; }
        public int Stock { get; set; }
        public double PrecioVenta { get; set; }
    }
}
