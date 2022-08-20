using Microsoft.AspNetCore.Mvc;
using Usuarios.Models;
using UsuarioHandlers;
using Usuarios.DTOS;

namespace UsuarioControllers
{
    [ApiController]
    [Route("[controller]")]

    public class UsuarioController : ControllerBase
    {
        [HttpGet(Name = "GetUsuarios")]
        public List<Usuario> ObtenerUsuarios([FromHeader] string NombreUsuario)
        {
            return UsuarioHandler.ObtenerUsuarios(NombreUsuario);
        }

        [HttpPost(Name = "PostUsuario")]
        public string CrearUsuario([FromBody] PostUsuario user)
        {
            return UsuarioHandler.InsertarUsuario(new Usuario
            {
                Nombre = user.Nombre,
                Apellido = user.Apellido,
                NombreUsuario = user.NombreUsuario,
                Contraseña = user.Contraseña,
                Mail = user.Mail
            });
        }

        [HttpPut(Name = "PutUsuario")]
        public string ModificarUsuario([FromBody] PutUsuario user)
        {
            return UsuarioHandler.ModificarUsuario(new Usuario
            {
                Id = user.ID,
                Nombre = user.Nombre,
                Apellido = user.Apellido,
                NombreUsuario = user.NombreUsuario,
                Contraseña = user.Contraseña,
                Mail = user.Mail
            });
        }

        [HttpDelete(Name = "DeleteUsuario")]
        public string BorrarUsuario([FromHeader] int user)
        {
            return UsuarioHandler.EliminarUsuario(user);
        }
    }
}
