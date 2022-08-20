namespace Ventas.DTOS
{
    //Intercambio de valores para CREAR (POST) una Venta
    public class PostVenta
    {
        public int IdUsuario { get; set; }
        public int IdProducto { get; set; }
        public int Stock { get; set; }
        public string Status { get; set; }
    }
}
