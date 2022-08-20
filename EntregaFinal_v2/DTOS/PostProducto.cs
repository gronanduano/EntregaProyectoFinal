namespace Productos.DTOS
{
    //Intercambio de valores para CREAR (POST) un Producto
    public class PostProducto
    {
        public string Descripciones { get; set; }
        public double Costo { get; set; }
        public double PrecioVenta { get; set; }
        public int Stock { get; set; }
        public int IdUsuario { get; set; }
        public string Status { get; set; }
    }
}

